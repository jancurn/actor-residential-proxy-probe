const _ = require('underscore');
const Apify = require('apify');
const request = require('request-promise');
const moment = require('moment');
const usZipCodeToDma = require('./us_zip_code_to_dma');

const { log } = Apify.utils;

const HEARTBEAT_INTERVAL_MILLIS = 20 * 1000;
const MAX_SESSION_AGE_MILLIS = 45 * 1000;
const NEW_SESSIONS_PER_HEARTBEAT = 30;

// Global state, which is periodically stored into the key-value store
let state;

// Dictionary of session keys currently being probed, to ensure we don't probe same ones in parallel.
// Key is sessionKey, value is true.
const sessionKeysInProgress = {};

// Increments the stats value
const statsInc = (propName) => {
    state.stats[propName] = (state.stats[propName] || 0) + 1;
};

// Generate random session key, always 9 chars long
const generateRandomSessionKey = () => {
    return Math.floor((Math.random() * 99999999999999999)).toString(16).substr(0, 9);
};

const fatalError = (err) => {
    log.exception(err, 'Fatal error');
    process.exit(1);
};

// TODO: We should have some fallback API for case keycdn.com is down...
const probeSession = async (sessionKey, countryCode) => {
    const opts = {
        url: 'https://tools.keycdn.com/geo.json',
        proxy: `http://groups-RESIDENTIAL,session-${sessionKey},country-${countryCode}:${process.env.APIFY_PROXY_PASSWORD}@proxy.apify.com:8000`,
        json: true,
        gzip: true,
    };
    const json = await request(opts);

    if (!json || !json.data || !json.data.geo || !json.data.geo.ip) throw new Error('Invalid response');
    const { geo } = json.data;

    return {
        ipAddress: geo.ip,
        countryCode: geo.country_code,
        regionName: geo.region_name,
        city: geo.city,
        postalCode: geo.postal_code,
    };
};

const handleSession = async (input, sessionKey, oldSessionInfo) => {
    // Is already in progress, skip it
    if (sessionKeysInProgress[sessionKey]) return;
    sessionKeysInProgress[sessionKey] = true;

    let sessionInfo;
    try {
        statsInc('probesInitiated');
        sessionInfo = await probeSession(sessionKey, input.countryCode);
    } catch (e) {
        console.log(`Session ${sessionKey}: Probe failed "${e}"`);
        statsInc('probesFailed');
        return;
    } finally {
        delete sessionKeysInProgress[sessionKey];
    }

    // console.log(`Session ${sessionKey}: ${JSON.stringify(sessionInfo)}`)

    let isNewIpAddress = false;
    if (oldSessionInfo && oldSessionInfo.ipAddress === sessionInfo.ipAddress && oldSessionInfo.foundAt) {
        // console.log(`Session ${sessionKey}: Still valid, will be reused`);
        sessionInfo.foundAt = moment(oldSessionInfo.foundAt).toDate();
    } else {
        isNewIpAddress = true;
        sessionInfo.foundAt = new Date();
        if (oldSessionInfo) console.log(`Session ${sessionKey}: IP address changed`); // from ${oldSessionInfo.ipAddress} (${oldSessionInfo.postalCode}) to ${sessionInfo.ipAddress} (${sessionInfo.postalCode})`);
    }

    // No postal code?
    if (!sessionInfo.postalCode) {
        console.log(`Session ${sessionKey}: Missing postal code ${JSON.stringify(_.pick(sessionInfo, 'ipAddress', 'regionName', 'city', 'postalCode'))}`);
        delete state.proxySessions[sessionKey];
        statsInc('missingPostalCode');
        return;
    }

    sessionInfo.dmaCode = input.countryCode === 'US' && usZipCodeToDma[sessionInfo.postalCode]
        ? usZipCodeToDma[sessionInfo.postalCode]
        : null;

    sessionInfo.lastProbedAt = new Date();

    // console.log(`Session ${sessionKey}: ${JSON.stringify(sessionInfo)}`);

    // If DMA or postal code is not in the requested set, forget the session and update stats
    if (input.dmaCodes) {
        if (!sessionInfo.dmaCode) {
            console.log(`Session ${sessionKey}: DMA code not found`);
            delete state.proxySessions[sessionKey];
            statsInc('dmaCodeNotFound');
            return;
        }
        if (!_.contains(input.dmaCodes, sessionInfo.dmaCode)) {
            console.log(`Session ${sessionKey}: DMA code not matching`);
            delete state.proxySessions[sessionKey];
            statsInc('dmaCodeMismatch');
            return;
        }

        if (isNewIpAddress) console.log(`Session ${sessionKey}: Matches DMA code ${sessionInfo.dmaCode} !!!`);
    } else if (input.postalCodes) {
        if (!_.contains(input.postalCodes, sessionInfo.postalCode)) {
            console.log(`Session ${sessionKey}: Postal code not matching`);
            delete state.proxySessions[sessionKey];
            statsInc('postalCodeMismatch');
            return;
        }

        if (isNewIpAddress) console.log(`Session ${sessionKey}: Matches postal code ${sessionInfo.postalCode} !!!`);
    }

    // Session matches the filter, save it
    state.proxySessions[sessionKey] = sessionInfo;
    statsInc('matched');
    return;
};


const heartbeat = ({ input, keyValueStore }) => {
    const regionToProxyCount = {};

    // First, iterate existing dmaCodessessions and launch their update in background
    for (let [sessionKey, sessionInfo] of Object.entries(state.proxySessions)) {
        handleSession(input, sessionKey, sessionInfo).catch(fatalError);

        // If session is not too old, consider it for region matching
        if (moment().diff(sessionInfo.lastProbedAt, 'millis') < MAX_SESSION_AGE_MILLIS) {
            if (input.dmaCodes) {
                regionToProxyCount[sessionInfo.dmaCode] = (regionToProxyCount[sessionInfo.dmaCode] || 0) + 1;
            } else {
                regionToProxyCount[sessionInfo.postalCode] = (regionToProxyCount[sessionInfo.postalCode] || 0) + 1;
            }
        }
    }

    // Check how many live sessions we have per region, and if not enough, then launch new ones
    const regions = input.dmaCodes ? input.dmaCodes : input.postalCodes;
    let minPerRegion = Number.POSITIVE_INFINITY;
    let maxPerRegion = Number.NEGATIVE_INFINITY;
    for (let region of regions) {
        const count = regionToProxyCount[region] || 0;
        minPerRegion = Math.min(count, minPerRegion);
        maxPerRegion = Math.max(count, maxPerRegion);
    }
    if (minPerRegion === Number.POSITIVE_INFINITY) minPerRegion = 0;
    if (maxPerRegion === Number.NEGATIVE_INFINITY) maxPerRegion = 0;

    const totalSessions = Object.keys(state.proxySessions).length;

    console.log(`Heartbeat: live sessions: ${totalSessions} of ${input.maxSessions}, minPerRegion: ${minPerRegion}, maxPerRegion: ${maxPerRegion}, regionsCount: ${regions.length}`);

    if (totalSessions < input.maxSessions && minPerRegion < input.minSessionsPerRegion) {
        console.log(`Probing ${NEW_SESSIONS_PER_HEARTBEAT} new sessions`);
        for (let i = 0; i < NEW_SESSIONS_PER_HEARTBEAT; i++) {
            handleSession(input, generateRandomSessionKey(), null).catch(fatalError);
        }
    }

    state.lastUpdatedAt = new Date();

    keyValueStore.setValue(input.recordKey, state).catch(fatalError);
};

Apify.main(async () => {
    const input = await Apify.getInput();

    // Pre-process and check selected regions
    input.dmaCodes = input.dmaCodes ? input.dmaCodes.trim().split(/\s+/g) : null;
    input.postalCodes = input.postalCodes ? input.postalCodes.trim().split(/\s+/g) : null;
    if ((!input.dmaCodes || !input.dmaCodes.length) && (!input.postalCodes || !input.postalCodes.length)) {
        throw new Error('Either "dmaCodes" or "postalCodes" input field must contain some values!');
    }

    const keyValueStore = await Apify.openKeyValueStore(input.keyValueStoreName);

    state = await keyValueStore.getValue(input.recordKey);
    if (!state || !_.isObject(state) || _.isArray(state)) {
        state = {
            stats: {},
            proxySessions: {},
        };
    }

    heartbeat({ input, keyValueStore, state });
    setInterval(() => {
        heartbeat({ input, keyValueStore, state });
    }, HEARTBEAT_INTERVAL_MILLIS);

    // Wait forever
    return new Promise(() => {});
});
