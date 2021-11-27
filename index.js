const asyncRedis = require('async-redis');
const commandLineArgs = require('command-line-args');
const commandLineUsage = require('command-line-usage');

const optionDefinitions = [
  { name: 'help', alias: 'h', type: Boolean },
  { name: 'from', alias: 'f', type: String, typeLabel: '{underline url}' },
  { name: 'to', alias: 't', type: String, typeLabel: '{underline url}' }
];

const options = commandLineArgs(optionDefinitions);

if (options.help || !options.from || !options.to) {
  console.log(commandLineUsage([
    {
      header: 'Synopsis',
      content: [
        '$ redis-migrate {bold --from} {underline url} {bold --to} {underline url}',
        '$ redis-migrate {bold --help}'
      ]
    },
    {
      header: 'Options',
      optionList: optionDefinitions
    }
  ]));

  process.exit(0);
}

const source = asyncRedis.createClient({
  url: options.from,
  return_buffers: true,
  tls: {
    rejectUnauthorized: false
  }
});
const destination = asyncRedis.createClient({
  url: options.to,
  return_buffers: true,
  tls: {
    rejectUnauthorized: false
  }
})

const imported = new Set();

async function copy(key, source, destination) {
  const ttl = await source.pttl(key);
  const value = await source.dump(key);

  await destination.restore(key, ttl < 0 ? 0 : ttl, value, 'REPLACE', 'ABSTTL');
}

(async () => {
  try {
    const total = await source.dbsize();
    let offset = '0';
  
    do {
      [offset, keys] = await source.scan(offset, 'COUNT', '100');
  
      for (let key of keys) {
        if (!imported.has(key)) {
          await copy(key, source, destination);
          imported.add(key);
          console.log(`Imported ${imported.size} / ${total}: ${key}`);
        }
      }
    } while (offset != '0')
  
  }
  finally {
    source.quit();
    destination.quit();
  }
})();