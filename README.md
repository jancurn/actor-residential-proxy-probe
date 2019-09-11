# Residential proxy probe

This actor finds residential IP address on [Apify Proxy](https://apify.com/proxy)
that are geolocated in specific postal codes or DMA areas.

The actor probes random sessions on Apify Proxy with the `RESIDENTIAL` proxy group
and using IP geolocation checks if the corresponding residential IP address belongs
to a certain postal code or DMA area, in a specific country.
If yes, the actor saves the session key and then performs periodic requests
on that session to keep it alive.
Therefore, the actor needs to run infinitely or as long as you need the proxies.

Yes, this actor is a hack.

The pool of residential proxy session is periodically stored as a JSON
record into a Key-value store (either to a named or an anonymous one),
including various statistics. The file looks as follows:

```json
{
  "stats": {
    "probesTotal": 1290,
    "probesMatched": 672,
    "probesDmaMismatch": 409,
    "probesDmaNotFound": 86,
    "refreshesTotal": 4688,
    "refreshesIpSame": 4197,
    "forgotten": 289,
    "probesFailed": 3,
    "refreshesFailed": 16,
    "refreshesIpChanged": 319,
    "probesNoPostalCode": 25
  },
  "proxySessions": {
    "596452102": {
      "ipAddress": "1.2.3.4",
      "countryCode": "US",
      "regionName": "New York",
      "city": "Yonkers",
      "postalCode": "10701",
      "dmaCode": "501",
      "foundAt": "2019-09-11T11:32:47.727Z",
      "lastCheckedAt": "2019-09-11T11:33:27.487Z"
    },
    "dbc0a42d7": {
      "ipAddress": "4.5.6.7",
      "countryCode": "US",
      "regionName": "Maryland",
      "city": "Severn",
      "postalCode": "21144",
      "dmaCode": "512",
      "foundAt": "2019-09-11T11:32:08.278Z",
      "lastCheckedAt": "2019-09-11T11:33:27.325Z"
    },
    ...
  }
}
``` 
