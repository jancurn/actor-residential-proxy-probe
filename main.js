const _ = require('underscore');
const Apify = require('apify');
const request = require('request-promise');
const moment = require('moment');
const usZipCodeToDma = require('./us_zip_code_to_dma');

const { log } = Apify.utils;

const HEARTBEAT_INTERVAL_MILLIS = 20 * 1000;
const STORE_STATE_INTERVAL_MILLIS = 10 * 1000;
const MAX_SESSION_AGE_MILLIS = 50 * 1000;
const NEW_SESSIONS_PER_HEARTBEAT = 30;

// Global state, which is periodically stored into the key-value store
let state;

// Dictionary of session keys currently being probed, to ensure we don't probe same ones in parallel.
// Key is sessionKey, value is true.
const sessionKeysBeingRefreshed = {};

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

const addNewSession = async (input) => {
    const sessionKey = generateRandomSessionKey();

    let sessionInfo;
    try {
        statsInc('probesTotal');
        sessionInfo = await probeSession(sessionKey, input.countryCode);
    } catch (e) {
        console.log(`Session ${sessionKey}: Probe failed "${e}"`);
        statsInc('probesFailed');
        return;
    }

    // console.log(`Session ${sessionKey}: ${JSON.stringify(sessionInfo)}`)

    // No postal code?
    if (!sessionInfo.postalCode) {
        console.log(`Session ${sessionKey}: Missing postal code ${JSON.stringify(_.pick(sessionInfo, 'ipAddress', 'regionName', 'city', 'postalCode'))}`);
        delete state.proxySessions[sessionKey];
        statsInc('probesNoPostalCode');
        return;
    }

    sessionInfo.dmaCode = input.countryCode === 'US' && usZipCodeToDma[sessionInfo.postalCode]
        ? usZipCodeToDma[sessionInfo.postalCode]
        : null;
    sessionInfo.foundAt = new Date();
    sessionInfo.lastCheckedAt = sessionInfo.foundAt;

    // console.log(`Session ${sessionKey}: ${JSON.stringify(sessionInfo)}`);

    // If DMA or postal code is not in the requested set, forget the session and update stats
    if (input.dmaCodes) {
        if (!sessionInfo.dmaCode) {
            console.log(`Session ${sessionKey}: DMA code not found`);
            delete state.proxySessions[sessionKey];
            statsInc('probesDmaNotFound');
            return;
        }
        if (!_.contains(input.dmaCodes, sessionInfo.dmaCode)) {
            console.log(`Session ${sessionKey}: DMA code not matching`);
            delete state.proxySessions[sessionKey];
            statsInc('probesDmaMismatch');
            return;
        }

        console.log(`Session ${sessionKey}: Matches DMA code ${sessionInfo.dmaCode} !!!`);
    } else if (input.postalCodes) {
        if (!_.contains(input.postalCodes, sessionInfo.postalCode)) {
            console.log(`Session ${sessionKey}: Postal code not matching`);
            delete state.proxySessions[sessionKey];
            statsInc('probesPostalCodeMismatch');
            return;
        }

        console.log(`Session ${sessionKey}: Matches postal code ${sessionInfo.postalCode} !!!`);
    }

    // Session matches the filter, save it
    state.proxySessions[sessionKey] = sessionInfo;
    statsInc('probesMatched');
};



const refreshExistingSession = async (input, sessionKey, sessionInfo) => {
    // If refresh already in progress, skip it
    if (sessionKeysBeingRefreshed[sessionKey]) return;
    sessionKeysBeingRefreshed[sessionKey] = true;

    let ipAddress;

    try {
        statsInc('refreshesTotal');

        const opts = {
            url: 'https://api.apify.com/v2/browser-info?skipHeaders=1',
            proxy: `http://groups-RESIDENTIAL,session-${sessionKey},country-${input.countryCode}:${process.env.APIFY_PROXY_PASSWORD}@proxy.apify.com:8000`,
            json: true,
            gzip: true,
        };
        const result = await request(opts);
        if (!result || !result.clientIp) throw new Error('Invalid response from Apify API');

        ipAddress = result.clientIp;
    } catch (e) {
        console.log(`Session ${sessionKey}: Refresh failed "${e}"`);
        statsInc('refreshesFailed');
        return;
    } finally {
        delete sessionKeysBeingRefreshed[sessionKey];
    }

    if (sessionInfo.ipAddress === ipAddress) {
        sessionInfo.lastCheckedAt = new Date();
        statsInc('refreshesIpSame');
        return;
    }

    console.log(`Session ${sessionKey}: IP address changed, forgetting it`);
    delete state.proxySessions[sessionKey];
    statsInc('refreshesIpChanged');
};


const heartbeat = ({ input, keyValueStore }) => {
    const regionToProxyCount = {};

    // First, iterate existing sessions and refresh them in background (send keep alive and validate IP is the same)
    for (let [sessionKey, sessionInfo] of Object.entries(state.proxySessions)) {
        refreshExistingSession(input, sessionKey, sessionInfo).catch(fatalError);

        // If session is not too old, consider it for region matching
        if (moment().diff(sessionInfo.lastCheckedAt, 'milliseconds') < MAX_SESSION_AGE_MILLIS) {
            const region = input.dmaCodes ? sessionInfo.dmaCode : sessionInfo.postalCode;
            const newCount = (regionToProxyCount[region] || 0) + 1;

            if (input.maxSessionsPerRegion && newCount > input.maxSessionsPerRegion) {
                console.log(`Session ${sessionKey}: Exceeded max session per region (${region}), will be forgotten `);
                delete state.proxySessions[sessionKey];
                statsInc('forgotten');
                continue;
            }

            regionToProxyCount[region] = newCount;
        }
    }

    // Check how many live sessions we have per region, and if not enough, then launch new ones
    const regions = input.dmaCodes ? input.dmaCodes : input.postalCodes;
    let minPerRegion = Number.POSITIVE_INFINITY;
    let maxPerRegion = Number.NEGATIVE_INFINITY;
    let minRegion;
    let maxRegion;
    for (let region of regions) {
        const count = regionToProxyCount[region] || 0;

        if (count < minPerRegion) {
            minPerRegion = count;
            minRegion = region;
        }
        if (count > maxPerRegion) {
            maxPerRegion = count;
            maxRegion = region;
        }
    }
    if (minPerRegion === Number.POSITIVE_INFINITY) minPerRegion = 0;
    if (maxPerRegion === Number.NEGATIVE_INFINITY) maxPerRegion = 0;

    const totalSessions = Object.keys(state.proxySessions).length;

    console.log(`Heartbeat: live sessions: ${totalSessions} of ${input.maxSessions}, minPerRegion: ${minPerRegion} (e.g. ${minRegion}), maxPerRegion: ${maxPerRegion} (e.g. ${maxRegion}), regionsCount: ${regions.length}`);

    if (totalSessions < input.maxSessions && minPerRegion < input.minSessionsPerRegion) {
        console.log(`Probing ${NEW_SESSIONS_PER_HEARTBEAT} new sessions`);
        for (let i = 0; i < NEW_SESSIONS_PER_HEARTBEAT; i++) {
            addNewSession(input).catch(fatalError);
        }
    }

    state.lastUpdatedAt = new Date();
};

const storeState = ({ input, keyValueStore }) => {
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

    heartbeat({ input, keyValueStore });
    setInterval(() => {
        heartbeat({ input, keyValueStore });
    }, HEARTBEAT_INTERVAL_MILLIS);

    // Store state in a more frequent interval
    storeState({ input, keyValueStore });
    setInterval(() => {
        storeState({ input, keyValueStore });
    }, STORE_STATE_INTERVAL_MILLIS);

    // Wait forever
    return new Promise(() => {});
});
