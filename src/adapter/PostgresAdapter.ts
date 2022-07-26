import fs from 'fs'
import path from 'path'
import { Client, ClientConfig } from 'pg'
import { ConstructedQuery } from '@sap/cds/apis/ql'
import liquibase from '../liquibase'
import { configOptions, liquibaseOptions } from '../config'
import { PostgresDatabase } from '../types/PostgresDatabase'
import { ChangeLog } from '../ChangeLog'
import { ViewDefinition } from '../types/AdapterTypes'
import { sortByCasadingViews } from '../util'
import { DataLoader } from '../DataLoader'

/**
 * Removes PostgreSQL specific view statements from the changelog, that may cloud deployments
 * to break.
 *
 * Revisit: Check why this is the case.
 *
 * @param {Changelog} changelog
 */
const removePostgreSystemViewsFromChangelog = (changelog) => {
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

export class PostgresAdapter {
  serviceKey: string

  options: configOptions

  logger: globalThis.Console

  cdsSQL: string[]

  cdsModel: any

  client: Client

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
  }

  async init() {
    this.client = new Client(this.getCredentialsForClient())
    await this.client.connect()
  }

  async exit() {
    this.client.end()
  }

  getCredentialsForClient() {
    const {
      service: { credentials },
    } = this.options

    const config: ClientConfig = {
      user: credentials.user || credentials.username,
      password: credentials.password,
      host: credentials.host || credentials.hostname,
      database: credentials.database || credentials.dbname,
      port: credentials.port,
    }

    if (credentials.sslrootcert) {
      config.ssl = {
        rejectUnauthorized: false,
        ca: credentials.sslrootcert,
      }
    }

    return config
  }

  /**
   * @override
   */
  async getSchemas(): Promise<string[]> {
    const response = await this.client.query('SELECT schema_name FROM information_schema.schemata;')
    const existingSchemas = response.rows as string[]
    return existingSchemas
  }

  /**
   * @override
   */
  async createDropSchemaFunction() {
    const {
      service: { credentials },
      migrations: {
        schema: { default: defaultSchema },
      },
    } = this.options

    await this.client.query(`SET search_path TO ${defaultSchema};`)

    let sql = fs.readFileSync(path.join(__dirname, './sql/drop_schema.sql')).toString()
    sql = sql.replace('postgres', credentials.user)

    await this.client.query(sql)

    this.logger.log(`[cds-dbm] - Drop Schema function created`)
  }

  /**
   * @override
   */
  async createCloneSchemaFunction() {
    const {
      service: { credentials },
      migrations: {
        schema: { default: defaultSchema },
      },
    } = this.options

    await this.client.query(`SET search_path TO ${defaultSchema};`)

    let sql = fs.readFileSync(path.join(__dirname, './sql/clone_schema.sql')).toString()
    sql = sql.replace('postgres', credentials.user)
    await this.client.query(sql)

    this.logger.log(`[cds-dbm] - Clone Schema function created`)
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
   * @override
   * @param changelog
   */
  // eslint-disable-next-line class-methods-use-this
  beforeDeploy(changelog: ChangeLog) {
    removePostgreSystemViewsFromChangelog(changelog)
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
  async dropViewsFromCloneDatabase(): Promise<void> {
    const {
      migrations: {
        schema: { clone: cloneSchema },
      },
    } = this.options

    await this.client.query(`SET search_path TO ${cloneSchema};`)

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

    await this.client.query(query)
  }

  /**
   * Returns the liquibase options for the given command.
   *
   * @override
   * @param {string} cmd
   */
  liquibaseOptionsFor(): liquibaseOptions {
    const { user, password, ssl, host, port, database } = this.getCredentialsForClient()
    let url = `jdbc:postgresql://${host}:${port}/${database}`

    if (ssl) {
      url += '?ssl=true'
    }

    return {
      username: user,
      password: password.toString(),
      referenceUrl: url,
      referenceUsername: user,
      referencePassword: password.toString(),
      url,
      classpath: `${__dirname}/../../drivers/postgresql-42.3.2.jar`,
      driver: 'org.postgresql.Driver',
    }
  }

  async synchronizeCloneDatabase(schema: string) {
    const cloneSchema = schema
    const temporaryChangelogFile = `${this.options.migrations.deploy.tmpFile}`

    await this.client.query(`DROP SCHEMA IF EXISTS ${cloneSchema} CASCADE`)
    await this.client.query(`CREATE SCHEMA ${cloneSchema}`)

    // Basically create a copy of the schema
    let options = this.liquibaseOptionsFor()
    options.defaultSchemaName = cloneSchema
    options.referenceDefaultSchemaName = this.options.migrations.schema!.default
    options.changeLogFile = temporaryChangelogFile

    await liquibase(options).run('diffChangeLog')

    // Remove unnecessary stuff
    const diffChangeLog = ChangeLog.fromFile(temporaryChangelogFile)
    removePostgreSystemViewsFromChangelog(diffChangeLog)
    diffChangeLog.toFile(temporaryChangelogFile)

    // Now deploy the copy to the clone
    options = this.liquibaseOptionsFor()
    options.defaultSchemaName = cloneSchema
    options.changeLogFile = temporaryChangelogFile

    await liquibase(options).run('update')

    fs.unlinkSync(temporaryChangelogFile)

    return Promise.resolve()
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
      const options = this.liquibaseOptionsFor()
      await liquibase(options).run('dropAll')
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
  async deploy({ autoUndeploy = false, loadMode = null, dryRun = false, createDb = false }) {
    this.logger.log(`[cds-dbm] - starting delta database deployment of service ${this.serviceKey}`)

    if (createDb) {
      await this.createDatabase()
      await this.createDropSchemaFunction()
      await this.createCloneSchemaFunction()
    }
    await this.initCds()

    const temporaryChangelogFile = `${this.options.migrations.deploy.tmpFile}`

    if (fs.existsSync(temporaryChangelogFile)) {
      fs.unlinkSync(temporaryChangelogFile)
    }

    const dirname = path.dirname(temporaryChangelogFile)

    if (!fs.existsSync(dirname)) {
      fs.mkdirSync(dirname)
    }

    // Setup the clone
    await this.synchronizeCloneDatabase(this.options.migrations.schema!.clone)

    // Drop the known views from the clone
    await this.dropViewsFromCloneDatabase()

    // Create the initial changelog
    const diffChangeLogOptions = {
      ...this.liquibaseOptionsFor(),
      defaultSchemaName: this.options.migrations.schema.default,
      referenceDefaultSchemaName: this.options.migrations.schema.clone,
      changeLogFile: temporaryChangelogFile,
    }

    await liquibase(diffChangeLogOptions).run('diffChangeLog')
    const dropViewsChangeLog = ChangeLog.fromFile(temporaryChangelogFile)
    fs.unlinkSync(temporaryChangelogFile)

    // Deploy the current state to the reference database
    await this.deployCdsToReferenceDatabase()

    // Update the changelog with the real changes and added views
    const liquibaseOptions2 = this.liquibaseOptionsFor()
    liquibaseOptions2.defaultSchemaName = this.options.migrations.schema.clone
    liquibaseOptions2.changeLogFile = temporaryChangelogFile

    await liquibase(liquibaseOptions2).run('diffChangeLog')

    const diffChangeLog = ChangeLog.fromFile(temporaryChangelogFile)

    // Merge the changelogs
    diffChangeLog.data.databaseChangeLog = dropViewsChangeLog.data.databaseChangeLog.concat(
      diffChangeLog.data.databaseChangeLog
    )

    // Process the changelog
    if (!autoUndeploy) {
      diffChangeLog.removeDropTableStatements()
    }
    diffChangeLog.addDropStatementsForUndeployEntities(this.options.migrations.deploy.undeployFile)

    const viewDefinitions = {}

    for (const changeLog of diffChangeLog.data.databaseChangeLog) {
      if (changeLog.changeSet.changes[0].dropView) {
        const { viewName } = changeLog.changeSet.changes[0].dropView

        // REVISIT: await in loop
        // eslint-disable-next-line no-await-in-loop
        viewDefinitions[viewName] = await this.getViewDefinition(viewName)
      }

      if (changeLog.changeSet.changes[0].createView) {
        const { viewName } = changeLog.changeSet.changes[0].createView
        viewDefinitions[viewName] = {
          name: viewName,
          definition: changeLog.changeSet.changes[0].createView.selectQuery,
        }
      }
    }

    diffChangeLog.reorderChangelog(viewDefinitions)

    // Call hooks
    this.beforeDeploy(diffChangeLog)

    diffChangeLog.toFile(temporaryChangelogFile)

    // Either log the update sql or deploy it to the database
    const updateCmd = dryRun ? 'updateSQL' : 'update'

    const updateOptions = { ...this.liquibaseOptionsFor(), changeLogFile: temporaryChangelogFile }

    const updateSQL: any = await liquibase(updateOptions).run(updateCmd)
    const newSchemas = []

    if (this.options.migrations.multitenant && this.options.migrations.schema.tenants) {
      const existingSchemas = await this.getSchemas()

      for (const tenant of this.options.migrations.schema.tenants) {
        const found = existingSchemas.find((schema: any) => schema.schema_name === tenant)

        if (found) {
          // Update Tenant Schema
          // REVISIT: keep await in loop for now, to get separated errors
          // eslint-disable-next-line no-await-in-loop
          await liquibase({ ...updateOptions, defaultSchemaName: tenant }).run(updateCmd)
          this.logger.log(`[cds-dbm] - Schema ${tenant} updated.`)
        } else {
          newSchemas.push(tenant)
        }
      }

      // Create Tenant Schemas
      for (const newSchema of newSchemas) {
        if (fs.existsSync(temporaryChangelogFile)) {
          fs.unlinkSync(temporaryChangelogFile)
        }

        if (!fs.existsSync(dirname)) {
          fs.mkdirSync(dirname)
        }

        // REVISIT: keep await in loop for now, to get separated errors
        // eslint-disable-next-line no-await-in-loop
        await this.synchronizeCloneDatabase(newSchema)

        this.logger.log(`[cds-dbm] - Schema ${newSchema} created.`)
      }
    }

    if (!dryRun) {
      this.logger.log(`[cds-dbm] - delta successfully deployed to the database`)

      if (loadMode) {
        // await this.load(loadMode.toLowerCase() === 'full')
      }
    } else {
      this.logger.log(updateSQL.stdout)
    }

    // unlink if not already completed through new tenant schemas
    if (newSchemas.length === 0) {
      fs.unlinkSync(temporaryChangelogFile)
    }
  }

  /*
   * Internal functions
   */

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
    this.cdsSQL.sort(sortByCasadingViews)
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
    const referenceSchema = this.options.migrations.schema!.reference

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
    const clientCredentials = this.getCredentialsForClient()
    const { database } = clientCredentials

    // Do not connect directly to the database
    delete clientCredentials.database

    try {
      // Revisit: should be more safe, but does not work
      // await this.client.query(`CREATE DATABASE $1`, [this.options.service.credentials.database])
      await this.client.query(`CREATE DATABASE ${database}`)
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

  }
}
