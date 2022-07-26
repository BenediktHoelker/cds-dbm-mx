/* eslint-disable no-await-in-loop */
import { array, boolean } from 'yargs'
import { config } from '../config'
import { PostgresAdapter } from '../adapter/PostgresAdapter'

const command = 'deploy [services]'
const desc = 'Dynamically identifies changes in your cds data model and deploys them to the database'
const builder = {
  service: {
    alias: 's',
    type: array,
    default: ['db'],
  },
  'auto-undeploy': {
    alias: 'a',
    type: boolean,
  },
  dry: {
    alias: 'd',
    type: boolean,
  },
  'load-via': {
    alias: 'l',
    type: String,
  },
  'create-db': {
    alias: 'c',
    type: boolean,
  },
}

const handler = async (argv: any) => {
  for (const service of argv.service) {
    const options = await config(service)
    const pgAdapter = new PostgresAdapter(service, options)

    await pgAdapter.init({
      createDb: argv.createDb,
    })

    await pgAdapter.deploy({
      autoUndeploy: argv.autoUndeploy,
      dryRun: argv.dry,
      loadMode: argv.loadVia,
    })

    pgAdapter.exit()
  }
}

export { command, desc, builder, handler }
