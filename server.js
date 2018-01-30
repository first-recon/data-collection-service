const express = require('express');
const https = require('https');
const request = require('request-promise-native');
const fs = require('fs');
const config = require('./config');

const server = express();

const INTERVAL_MS = config.interval * 3600000; // convert hrs -> ms

const state = {
  currentState: 'IDLE'
};

server.get('/status', (req, res) => {
  res.send(state);
});

server.listen(3000, () => {
  console.log('Data collection service running, status endpoint on port 3000...');

  // SQL Team schema
  // integer id, integer number, varchar name, boolean saved (if team is saved we don't push changes to it's *name* from FIRST)

  // map FIRST's team format to Recon's, stripping out any data we don't need
  function convertFormat(source) {
    return {
      id: source.id,
      number: source.team_number_yearly,
      name: source.team_name_calc
    };
  }

  // set up task to scrape FIRST's database for updates
  (function fetchUpdates() {
    console.log('fetching updates...');
    state.currentState = 'DOWNLOADING'; 

    // pls renew your SSL cert first...
    request({
      url: config.url,
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
    .then(esData => esData.hits.hits.map(item => convertFormat(item._source)))
    .then((db) => {
      fs.writeFileSync('teams.json', JSON.stringify(db));
      state.currentState = 'IDLE';
      console.log(`Update complete. Next update in ${config.interval} hours.`);
      setTimeout(fetchUpdates, INTERVAL_MS);
    })
    .catch((error) => {
      console.log(error);
      console.log(`Request failed with above error, retrying in ${config.interval} hours...`);
      setTimeout(fetchUpdates, config.interval);
    });
  })();
});

