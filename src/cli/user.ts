import "dotenv/config";
import { mongoConfig } from "../config/mongo.config.js";
import { hashPassword } from "../auth/password.js";
import { MongoUserStore } from "../auth/users.js";

const USAGE = `Usage:
  npm run user -- add <username>       create a user, or reset its password
  npm run user -- disable <username>   block sign-in (takes effect on the next request)
  npm run user -- enable <username>
  npm run user -- list

The password is asked twice and not shown. For scripts, set GATE_PASSWORD instead.`;

/** Reads a line without echoing it, for passwords. */
function askHidden(question: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const input = process.stdin;
    if (!input.isTTY) return reject(new Error('No terminal to ask for a password; set GATE_PASSWORD'));
    process.stdout.write(question);
    input.setRawMode(true); input.resume(); input.setEncoding('utf8');
    let value = '';
    const onData = (chunk: string) => {
      for (const char of chunk) {
        if (char === '\u0003') { input.setRawMode(false); process.stdout.write('\n'); process.exit(130); }
        if (char === '\r' || char === '\n') { input.setRawMode(false); input.pause(); input.off('data', onData); process.stdout.write('\n'); return resolve(value); }
        if (char === '\u007f' || char === '\b') value = value.slice(0, -1); else value += char;
      }
    };
    input.on('data', onData);
  });
}

async function readPassword(): Promise<string> {
  if (process.env['GATE_PASSWORD']) return process.env['GATE_PASSWORD'];
  const first = await askHidden('Password (12 characters or more): ');
  const second = await askHidden('Repeat password: ');
  if (first !== second) throw new Error('The two passwords are different');
  return first;
}

async function main(): Promise<number> {
  const [command, username] = process.argv.slice(2);
  if (!command || ['-h', '--help', 'help'].includes(command)) { console.log(USAGE); return command ? 0 : 1; }
  await mongoConfig.connect();
  const users = new MongoUserStore(mongoConfig);
  try {
    switch (command) {
      case 'add': {
        if (!username) { console.error(USAGE); return 1; }
        const result = await users.upsert(username, await hashPassword(await readPassword()));
        console.log(`${result === 'created' ? 'Created' : 'Updated'} ${username.toLowerCase()}.`);
        return 0;
      }
      case 'disable': case 'enable': {
        if (!username) { console.error(USAGE); return 1; }
        const found = await users.setDisabled(username, command === 'disable');
        console.log(found ? `${command === 'disable' ? 'Disabled' : 'Enabled'} ${username.toLowerCase()}.` : `No user named ${username}.`);
        return found ? 0 : 1;
      }
      case 'list': {
        const all = await users.list();
        if (!all.length) console.log('No users yet. Add one with: npm run user -- add <username>');
        for (const u of all) console.log(`${u.username.padEnd(24)} ${u.disabled ? 'disabled' : 'active'}`);
        return 0;
      }
      default: console.error(USAGE); return 1;
    }
  } finally { await mongoConfig.disconnect(); }
}

main().then(code => process.exit(code), error => { console.error(error instanceof Error ? error.message : error); process.exit(1); });
