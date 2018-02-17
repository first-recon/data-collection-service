const env = process.argv[2];

module.exports = require(`./${env || 'production'}`);