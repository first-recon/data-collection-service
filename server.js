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

const createEventQuery = (args) => `insert into events (id, name, venue, type, season, date_start, date_end, location_street, location_postalCode, location_city, location_state, location_country) values (${args.join(',')})`;
const createTeamQuery = (args) => `insert into teams (id, name, number) values (${args.join(',')})`;

function save (table, items) {
  return new Promise((resolve, reject) => {
    Promise.all(items.map((item) => {
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
      return new Promise((res, rej) => {
        client.query(query, (error, results) => {
          if (error) {
            console.log(`Error with query: ${query}`);
            console.log(error);
            rej(error);
          } else {
            res(results.rows);
          }
        });
      });
    })).then(resolve).catch(console.log);
  });
}

server.get('/status', (req, res) => {
  res.send(state);
});

server.listen(3000, () => {
  console.log('Data collection service running, status endpoint on port 3000...');

  // map FIRST's data formats to Recon's, stripping out any data we don't need
  function convertFormat(type, source) {
    if (type === 'team') {
      return {
        id: source.id,
        number: source.team_number_yearly,
        name: source.team_name_calc
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
      return `https://es01.usfirst.org/events/_search?size=${size}&from=0&source={"query":{"filtered":{"query":{"bool":{"must":[{"bool":{"should":[[{"match":{"event_type":"FTC"}}]]}},{"bool":{"should":[[{"match":{"fk_program_seasons":"251"}},{"match":{"fk_program_seasons":"249"}},{"match":{"fk_program_seasons":"253"}},{"match":{"fk_program_seasons":"247"}}]]}},{"range":{"date_end":{"gte":"2017-09-01","lte":"2018-09-01"}}}]}}}},"sort":"event_name.raw"}`;
    } else if (type === 'teams') {
      return `https://es01.usfirst.org/teams/_search?size=${size}&from=0&source={"query":{"filtered":{"query":{"bool":{"must":[{"bool":{"should":[[{"match":{"team_type":"FTC"}}]]}},{"bool":{"should":[[{"match":{"fk_program_seasons":"251"}},{"match":{"fk_program_seasons":"249"}},{"match":{"fk_program_seasons":"253"}},{"match":{"fk_program_seasons":"247"}}]]}}]}}}},"sort":"team_nickname.raw"}`;
    }
  }

  // set up task to scrape FIRST's database for updates
  (function fetchUpdates() {
    console.log('fetching updates...');
    state.currentState = 'DOWNLOADING'; 

    // pls renew your SSL cert first...
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
      .then((teams) => save('teams', teams))
    .then(() => {
      return request({
        url: getRequestUrl('events', config.size),
        method: 'GET',
        agent: new https.Agent({
          host: 'es01.usfirst.org',
          port: 443,
          path: '/',
          rejectUnauthorized: false
        })
      });
    })
    .then(JSON.parse)
    .then(esData => esData.hits.hits.map(item => convertFormat('event', item._source)))
    .then((events) => {
      return save('events', events)
        .then(() => {
          // fs.writeFileSync('events.json', JSON.stringify(events));
          state.currentState = 'IDLE';
          console.log(`Update complete. Next update in ${config.interval} hours.`);
          setTimeout(fetchUpdates, INTERVAL_MS);
        });
    })
    .catch((error) => {
      console.log(error);
      console.log(`Request failed with above error, retrying in ${config.interval} hours...`);
      setTimeout(fetchUpdates, INTERVAL_MS);
    });
  })();
});

