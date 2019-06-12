const express = require('express');
const https = require('https');
const request = require('request-promise-native');
const fs = require('fs');
const pgEscape = require('pg-escape');
const { Client } = require('pg');
const config = require('./config');

const client = new Client(config.postgres);
client.connect();

const server = express();

const INTERVAL_MS = config.interval * 3600000; // convert hrs -> ms

const state = {
  currentState: 'IDLE'
};

const eventReq = {
  'query': {
    'filtered': {
      'query': {
        'bool': {
          'must': [
            {
              'bool': {
                'should': [
                  [
                    {
                      'match': {
                        'event_type': 'FTC'
                      }
                    }
                  ]
                ]
              }
            },
            {
              'range': {
                'date_end': {
                  'gte': config.date_range.start,
                  'lte': config.date_range.end
                }
              }
            }
          ]
        }
      }
    }
  },
  'sort': 'event_name.raw'
};

const createEventQuery = (args) => `insert into events.events (id, name, venue, type, season, date_start, date_end, location_street, location_postalCode, location_city, location_state, location_country) values (${args.join(',')})`;
const createTeamQuery = (args) => `insert into teams.teams (id, name, number) values (${args.join(',')})`;

function save (table, items) {
  items.map((item) => {
    if (table === 'teams') {
      const { id, name, number } = item;
      return createTeamQuery([id, `${pgEscape.dollarQuotedString(name)}`, number]);
    } else if (table === 'events') {
      const { id, name, venue, type, season, date, location } = item;
      return createEventQuery([
        id,
        `${pgEscape.dollarQuotedString(name)}`,
        `${pgEscape.dollarQuotedString(venue)}`,
        `${pgEscape.dollarQuotedString(type)}`,
        season,
        new Date(date.start).getTime() / 1000,
        new Date(date.end).getTime() / 1000,
        `${pgEscape.dollarQuotedString(location.street)}`,
        location.postalCode,
        `${pgEscape.dollarQuotedString(location.city)}`,
        `${pgEscape.dollarQuotedString(location.state)}`,
        `${pgEscape.dollarQuotedString(location.country)}`
      ]);
    }
  }).map((query) => {
    client.query(query, (error, results) => {
      if (error) {
        console.log(`Error with query: ${query}`);
      } else {
        console.log(`finished fetching updates for ${table}`);
      }
    });
  });
}

server.get('/status', (req, res) => {
  res.send(state);
});

server.listen(config.port, () => {
  console.log('Data collection service running, status endpoint on port 3000...');

  // map FIRST's data formats to Recon's, stripping out any data we don't need
  function convertFormat(type, source) {
    if (type === 'team') {
      return {
        id: source.id,
        number: source.team_number_yearly,
        name: source.team_nickname
      };
    } else if (type === 'event') {
      return {
        id: source.id,
        name: source.event_name,
        venue: source.event_code,
        date: {
          start: source.date_start,
          end: source.date_end
        },
        season: source.event_season,
        type: source.event_subtype,
        location: {
          street: source.event_address1,
          postalCode: source.event_postalcode,
          city: source.event_city,
          state: source.event_stateprov,
          country: source.countryCode
        }
      };
    }
  }

  function getRequestUrl(type, size=10000) {
    if (type === 'events') {
      return `https://es01.usfirst.org/events/_search?size=${size}&from=0&source=${JSON.stringify(eventReq)}`;
    } else if (type === 'teams') {
      return `https://es01.usfirst.org/teams/_search?size=${size}&from=0&source={"query":{"filtered":{"query":{"bool":{"must":[{"bool":{"should":[[{"match":{"team_type":"FTC"}}]]}},{"bool":{"should":[[{"match":{"fk_program_seasons":"251"}},{"match":{"fk_program_seasons":"249"}},{"match":{"fk_program_seasons":"253"}},{"match":{"fk_program_seasons":"247"}}]]}}]}}}},"sort":"team_nickname.raw"}`;
    }
  }

  // set up task to scrape FIRST's database for updates
  (function fetchUpdates() {
    console.log('fetching updates...');
    state.currentState = 'DOWNLOADING'; 

    // pls renew your SSL cert first...
    if (config.sync_tables.teams) {
      request({
        url: getRequestUrl('teams', config.size),
        method: 'GET',
        agent: new https.Agent({
          host: 'es01.usfirst.org',
          port: 443,
          path: '/',
          rejectUnauthorized: false
        })
      })
      .then((response) => {
        state.currentState = 'PROCESSING';
        return JSON.parse(response);
      })
      .then(esData => esData.hits.hits.map(item => convertFormat('team', item._source)))
      .then((teams) => {
        save('teams', teams);
      });
    }

    if (config.sync_tables.events) {
      request({
        url: getRequestUrl('events', config.size),
        method: 'GET',
        agent: new https.Agent({
          host: 'es01.usfirst.org',
          port: 443,
          path: '/',
          rejectUnauthorized: false
        })
      })
      .then(JSON.parse)
      .then(esData => esData.hits.hits.map(item => convertFormat('event', item._source)))
      .then((events) => {
        save('events', events);
      });
    }
  })();
});

