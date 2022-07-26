import fs from 'fs'
import path from 'path'
import { Client, ClientConfig } from 'pg'
import { ConstructedQuery } from '@sap/cds/apis/ql'
import { configOptions, liquibaseOptions } from '../config'
import liquibase from '../liquibase'

import { PostgresDatabase } from '../types/PostgresDatabase'
import { ChangeLog } from '../ChangeLog'
import { ViewDefinition } from '../types/AdapterTypes'
import { sortByCascadingViews } from '../util'
import { DataLoader } from '../DataLoader'

/**
 * Removes PostgreSQL specific view statements from the changelog, that may cloud deployments
 * to break.
 *
 * Revisit: Check why this is the case.
 *
 * @param {Changelog} changelog
 */
function removePostgreSystemViewsFromChangelog(changelog) {
  for (const changeLog of changelog.data.databaseChangeLog) {
    changeLog.changeSet.changes = changeLog.changeSet.changes.filter((change) => {
      return (
        !(change.createView && change.createView.viewName.includes('pg_stat_statements')) &&
        !(change.dropView && change.dropView.viewName.includes('pg_stat_statements')) &&
        !(change.createView && change.createView.viewName.includes('pg_buffercache')) &&
        !(change.dropView && change.dropView.viewName.includes('pg_buffercache'))
      )
    })
  }
}

function initChangeLogFile(name: string): string {
  const fileName = `tmp/_${name}.json`
  const dirname = path.dirname(fileName)

  if (fs.existsSync(fileName)) {
    fs.unlinkSync(fileName)
  }

  if (!fs.existsSync(dirname)) {
    fs.mkdirSync(dirname)
  }

  return fileName
}

export class PostgresAdapter {
  serviceKey: string

  options: configOptions

  logger: globalThis.Console

  liquibase: any

  cdsSQL: string[]

  cdsModel: any

  client: Client

  referenceSchema: '_cdsdbm_ref'

  defaultSchema: 'public'

  /**
   * The constructor
   *
   * @param serviceKey
   * @param options
   */
  constructor(serviceKey: string, options: configOptions) {
    this.serviceKey = serviceKey
    this.options = options
    this.logger = global.console
    this.liquibase = liquibase(this.getLiquibaseOptions())
  }

  async init({ createDb = false }) {
    if (createDb) {
      await this.createDatabase()
      await this.createSQLFunction('clone_schema.sql')
      await this.createSQLFunction('drop_schema.sql')
    } else {
      this.client = new Client(this.getCredentialsForClient())
      await this.client.connect()
    }
  }

  async exit() {
    this.client.end()
  }

  getCredentialsForClient() {
    const {
      service: { credentials },
    } = this.options

    const clientConfig: ClientConfig = {
      user: credentials.user || credentials.username,
      password: credentials.password,
      host: credentials.host || credentials.hostname,
      database: credentials.database || credentials.dbname,
      port: credentials.port,
    }

    if (credentials.sslrootcert) {
      clientConfig.ssl = {
        rejectUnauthorized: false,
        ca: credentials.sslrootcert,
      }
    }

    return clientConfig
  }

  /**
   * Returns the liquibase options for the given command.
   *
   * @override
   * @param {string} cmd
   */
  getLiquibaseOptions(): liquibaseOptions {
    const { user, password, ssl, host, port, database } = this.getCredentialsForClient()
    let url = `jdbc:postgresql://${host}:${port}/${database}`

    if (ssl) {
      url += '?ssl=true'
    }

    return {
      url,
      username: user,
      password: password.toString(),
      referenceUrl: url,
      referenceUsername: user,
      referencePassword: password.toString(),
    }
  }

  /**
   * @override
   */
  async getSchemas(): Promise<string[]> {
    const {
      migrations: {
        schema: { default: defaultSchema },
      },
    } = this.options
    const response = await this.client.query('SELECT schema_name FROM information_schema.schemata;')
    // eslint-disable-next-line camelcase
    const existingSchemas = response.rows.map(({ schema_name }) => schema_name)

    existingSchemas.push(defaultSchema)
    return existingSchemas
  }

  /**
   * @override
   */
  async createSQLFunction(name: string) {
    const {
      service: { credentials },
      migrations: {
        schema: { default: defaultSchema },
      },
    } = this.options

    let sql = fs.readFileSync(path.join(__dirname, `./sql/${name}`)).toString()
    sql = sql.replace('postgres', credentials.user)

    await this.client.query(`SET search_path TO ${defaultSchema};`)
    await this.client.query(sql)

    this.logger.log(`[cds-dbm] - ${name} function created`)
  }

  /**
   *
   * @override
   * @param table
   */
  async truncateTable(table: any): Promise<void> {
    await this.client.query(`TRUNCATE ${table} RESTART IDENTITY`)
  }

  /**
   *
   */
  async dropViewsFromSchema(schema: string): Promise<void> {
    const queries = this.cdsSQL
      .map((query) => {
        const [, table, entity] = query.match(/^\s*CREATE (?:(TABLE)|VIEW)\s+"?([^\s(]+)"?/im) || []
        if (!table) {
          return `DROP VIEW IF EXISTS ${entity} CASCADE`
        }
        return ''
      })
      .filter(Boolean)

    const query = queries.join(';')

    await this.client.query(`SET search_path TO ${schema};`)
    await this.client.query(query)
  }

  /*
   * API functions
   */

  /**
   * Drop tables and views from the database. If +dropAll+ is
   * true, then the whole schema is dropped including non CDS
   * tables/views.
   *
   * @param {boolean} dropAll
   */
  public async drop({ dropAll = false }) {
    if (dropAll) {
      await this.liquibase.run('dropAll')
    } else {
      await this.dropCdsEntitiesFromDatabase(this.serviceKey, false)
    }
    return Promise.resolve()
  }

  // /**
  //  *
  //  * @param {boolean} isFullMode
  //  */
  // public async load(isFullMode = false) {
  //   await this.initCds()
  //   const loader = new DataLoader(this, isFullMode)
  //   // TODO: Make more flexible
  //   await loader.loadFrom(['data', 'csv'])
  // }

  /**
   * Identifies the changes between the cds definition and the database, generates a delta and deploys
   * this to the database.
   * We use a clone and reference schema to identify the delta, because we need to initially drop
   * all the views and we do not want to do this with a potential production database.
   *
   */
  async deploy(args) {
    const {
      migrations: {
        schema: { tenants },
        multitenant: isMultitenant,
      },
    } = this.options

    this.logger.log(`[cds-dbm] - starting delta database deployment of service ${this.serviceKey}`)

    await this.initCds()

    // Deploy the current state to the reference database
    await this.deployCdsToReferenceDatabase()

    const existingSchemas = await this.getSchemas()

    if (isMultitenant && tenants) {
      const queryCreateNewSchemas = tenants
        .filter((tenant) => !existingSchemas.includes(tenant))
        .map((tenant) => `CREATE SCHEMA ${tenant};`)
        .join()

      await this.client.query(queryCreateNewSchemas)
    }

    for (const tenant of tenants) {
      // eslint-disable-next-line no-await-in-loop
      await this.updateSchema({ schema: tenant, ...args })
    }

    if (!args.dryRun) {
      this.logger.log(`[cds-dbm] - delta successfully deployed to the database`)

      if (args.loadMode) {
        // await this.load(loadMode.toLowerCase() === 'full')
      }
    }
  }

  async updateSchema({ schema, autoUndeploy = false, undeployFile }) {
    // Setup the clone
    const cloneSchema = await this.createClone(schema)

    // Drop the known views from the clone
    await this.dropViewsFromSchema(cloneSchema)

    const dropViewsChangeLogFile = initChangeLogFile('dropViews')

    // Create the initial changelog
    await this.liquibase.run('diffChangeLog', {
      defaultSchemaName: cloneSchema,
      referenceDefaultSchemaName: schema,
      changeLogFile: dropViewsChangeLogFile,
    })

    const dropViewsChangeLog = ChangeLog.fromFile(dropViewsChangeLogFile)

    const diffChangeLogFile = initChangeLogFile('diff')

    // Update the changelog with the real changes and added views
    await this.liquibase.run('diffChangeLog', {
      defaultSchemaName: schema,
      referenceDefaultSchemaName: this.referenceSchema,
      changeLogFile: diffChangeLogFile,
    })

    const diffChangeLog = ChangeLog.fromFile(diffChangeLogFile)

    // Merge the changelogs
    const changeLogs = [...diffChangeLog.data.databaseChangeLog, dropViewsChangeLog.data.databaseChangeLog]

    // Process the changelog
    if (!autoUndeploy) {
      diffChangeLog.removeDropTableStatements()
    }

    diffChangeLog.addDropStatementsForUndeployEntities(undeployFile)

    const viewDefinitions = {}

    for (const changeLog of changeLogs) {
      if (changeLog.changeSet?.changes[0]?.dropView) {
        const { viewName } = changeLog.changeSet.changes[0].dropView

        // REVISIT: await in loop
        // eslint-disable-next-line no-await-in-loop
        viewDefinitions[viewName] = await this.getViewDefinition(viewName)
      }

      if (changeLog.changeSet?.changes[0]?.createView) {
        const { viewName } = changeLog.changeSet.changes[0].createView
        viewDefinitions[viewName] = {
          name: viewName,
          definition: changeLog.changeSet.changes[0].createView.selectQuery,
        }
      }
    }

    diffChangeLog.reorderChangelog(viewDefinitions)

    removePostgreSystemViewsFromChangelog(diffChangeLog)

    diffChangeLog.toFile(diffChangeLogFile)

    await this.liquibase.run('update', {
      changeLogFile: diffChangeLogFile,
    })
  }

  async createClone(schema: string): Promise<string> {
    const cloneSchema = `_clone_${schema}`
    const cloneChangeLogFile = initChangeLogFile(cloneSchema)

    await this.client.query(`DROP SCHEMA IF EXISTS ${cloneSchema} CASCADE; CREATE SCHEMA ${cloneSchema};`)

    await this.liquibase.run('diffChangeLog', {
      defaultSchemaName: cloneSchema,
      referenceDefaultSchemaName: schema,
      changeLogFile: cloneChangeLogFile,
    })

    // Remove unnecessary stuff
    const diffChangeLog = ChangeLog.fromFile(cloneChangeLogFile)
    removePostgreSystemViewsFromChangelog(diffChangeLog)
    diffChangeLog.toFile(cloneChangeLogFile)

    await this.liquibase.run('update', {
      defaultSchemaName: cloneSchema,
      changeLogFile: cloneChangeLogFile,
    })

    return cloneSchema
  }

  async getViewDefinition(viewName: string): Promise<ViewDefinition> {
    const {
      migrations: {
        schema: { default: defaultSchema },
      },
    } = this.options

    const query =
      `SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '${defaultSchema}' AND table_name = $1 ORDER BY table_name;` as string
    const { rows } = await this.client.query(query, [viewName])

    const pattern = `${defaultSchema}.`
    const regex = new RegExp(pattern, 'g')

    const viewDefinition: ViewDefinition = {
      name: viewName,
      definition: rows[0]?.view_definition?.replace(regex, ''),
    }

    return viewDefinition
  }

  /**
   * Initialize the cds model (only once)
   */
  private async initCds() {
    try {
      this.cdsModel = await cds.load(this.options.service.model)
    } catch (error) {
      throw new Error(`[cds-dbm] - failed to load model ${this.options.service.model}`)
    }

    this.cdsSQL = cds.compile.to.sql(this.cdsModel) as unknown as string[]
    this.cdsSQL.sort(sortByCascadingViews)
  }

  /**
   * Drops all known views (and tables) from the database.
   *
   * @param {string} service
   */
  protected async dropCdsEntitiesFromDatabase(service: string, viewsOnly = true) {
    const model = await cds.load(this.options.service.model)
    const cdssql = cds.compile.to.sql(model)
    const dropViews = []
    const dropTables = []

    for (const each of cdssql) {
      const [, table, entity] = each.match(/^\s*CREATE (?:(TABLE)|VIEW)\s+"?([^\s(]+)"?/im) || []
      if (!table) {
        dropViews.push({ DROP: { view: entity } })
      }
      if (!viewsOnly && table) {
        dropTables.push({ DROP: { table: entity } })
      }
    }

    const tx = cds.services[service].transaction({})
    await tx.run(dropViews as unknown as ConstructedQuery)
    await tx.run(dropTables as unknown as ConstructedQuery)

    return tx.commit()
  }

  /**
   * @override
   */
  async deployCdsToReferenceDatabase() {
    const { referenceSchema } = this

    await this.client.query(`DROP SCHEMA IF EXISTS ${referenceSchema} CASCADE`)
    await this.client.query(`CREATE SCHEMA ${referenceSchema}`)
    await this.client.query(`SET search_path TO ${referenceSchema};`)

    const serviceInstance = cds.services[this.serviceKey] as PostgresDatabase

    const query = this.cdsSQL.map((q) => serviceInstance.cdssql2pgsql(q)).join(' ')

    await this.client.query(query)
  }

  /**
   * @override
   */
  async createDatabase() {
    const { database, ...clientCredentials } = this.getCredentialsForClient()

    const client = new Client(clientCredentials)
    await client.connect()

    try {
      // Revisit: should be more safe, but does not work
      // await this.client.query(`CREATE DATABASE $1`, [this.options.service.credentials.database])
      await client.query(`CREATE DATABASE ${database}`)
      this.logger.log(`[cds-dbm] - created database ${database}`)
    } catch (error) {
      switch (error.code) {
        case '42P04': // already exists
          this.logger.log(`[cds-dbm] - database ${database} is already present`)
          break
        case '23505': // concurrent attempt
          break
        default:
          throw error
      }
    }

    client.end()

    // else-block in init-code (with DB-name)
    this.client = new Client(this.getCredentialsForClient())
    await this.client.connect()
  }
}
