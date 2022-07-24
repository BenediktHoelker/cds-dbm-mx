import { Client, ClientConfig } from 'pg'
import fs from 'fs'
import path from 'path'
import liquibase from '../liquibase'
import { BaseAdapter } from './BaseAdapter'
import { liquibaseOptions } from '../config'
import { PostgresDatabase } from '../types/PostgresDatabase'
import { ChangeLog } from '../ChangeLog'
import { ViewDefinition } from '../types/AdapterTypes'

/**
 * Removes PostgreSQL specific view statements from the changelog, that may cloud deployments
 * to break.
 *
 * Revisit: Check why this is the case.
 *
 * @param {Changelog} changelog
 */
const _removePostgreSystemViewsFromChangelog = (changelog) => {
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

const getCredentialsForClient = (credentials) => {
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

export class PostgresAdapter extends BaseAdapter {
  /**
   * @override
   */
  async _cloneSchema(tenant: string) {
    const { credentials } = this.options.service
    const defaultSchema = this.options.migrations.schema!.default
    const client = new Client(getCredentialsForClient(credentials))
    const sql = `SELECT clone_schema('${defaultSchema}', '${tenant}', '0');` as string

    await client.connect()
    await client.query(sql)

    this.logger.log(`[cds-dbm] - Schema ${tenant} created.`)
    client.end()
  }

  /**
   * @override
   */
  async _getSchemas(): Promise<string[]> {
    const { credentials } = this.options.service
    const client = new Client(getCredentialsForClient(credentials))

    await client.connect()
    const response = await client.query('SELECT schema_name FROM information_schema.schemata;')
    client.end()

    const existingSchemas = response.rows as string[]
    return existingSchemas
  }

  /**
   * @override
   */
  async _createDropSchemaFunction() {
    const { credentials } = this.options.service
    const defaultSchema = this.options.migrations.schema!.default
    const client = new Client(getCredentialsForClient(credentials))

    await client.connect()
    await client.query(`SET search_path TO ${defaultSchema};`)
    try {
      let sql = fs.readFileSync(path.join(__dirname, './sql/drop_schema.sql')).toString()
      sql = sql.replace('postgres', credentials.user)
      await client.query(sql)
      // this.logger.log(`[cds-dbm] - Drop Schema function created`)
    } catch (error) {
      switch (error.code) {
        default:
          throw error
      }
    }
    client.end()
  }

  /**
   * @override
   */
  async _createCloneSchemaFunction() {
    const { credentials } = this.options.service
    const defaultSchema = this.options.migrations.schema!.default
    const client = new Client(getCredentialsForClient(credentials))
    await client.connect()
    await client.query(`SET search_path TO ${defaultSchema};`)

    try {
      let sql = fs.readFileSync(path.join(__dirname, './sql/clone_schema.sql')).toString()
      sql = sql.replace('postgres', credentials.user)
      await client.query(sql)
      // this.logger.log(`[cds-dbm] - Clone Schema function created`)
    } catch (error) {
      switch (error.code) {
        default:
          throw error
      }
    }
    client.end()
  }

  async getViewDefinition(viewName: string): Promise<ViewDefinition> {
    const { credentials } = this.options.service
    const schema = this.options.migrations.schema?.default
    const query =
      `SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '${schema}' AND table_name = $1 ORDER BY table_name;` as string
    const client = new Client(getCredentialsForClient(credentials))

    await client.connect()
    const { rows } = await client.query(query, [viewName])
    await client.end()

    const pattern = `${schema}.`
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
    _removePostgreSystemViewsFromChangelog(changelog)
  }

  /**
   *
   * @override
   * @param table
   */
  async _truncateTable(table: any): Promise<void> {
    const { credentials } = this.options.service
    const client = new Client(getCredentialsForClient(credentials))

    await client.connect()
    await client.query(`TRUNCATE ${table} RESTART IDENTITY`)
    client.end()
  }

  /**
   *
   */
  async _dropViewsFromCloneDatabase(): Promise<void> {
    const { credentials } = this.options.service
    const cloneSchema = this.options.migrations.schema!.clone
    const client = new Client(getCredentialsForClient(credentials))

    await client.connect()
    await client.query(`SET search_path TO ${cloneSchema};`)

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

    await client.query(query)

    return client.end()
  }

  /**
   * Returns the liquibase options for the given command.
   *
   * @override
   * @param {string} cmd
   */
  liquibaseOptionsFor(cmd: string): liquibaseOptions {
    const { credentials } = this.options.service
    let url = `jdbc:postgresql://${credentials.host || credentials.hostname}:${credentials.port}/${
      credentials.database || credentials.dbname
    }`
    if (credentials.sslrootcert) {
      url += '?ssl=true'
    }

    const options: liquibaseOptions = {
      username: credentials.user || credentials.username,
      password: this.options.service.credentials.password,
      url,
      classpath: `${__dirname}/../../drivers/postgresql-42.3.2.jar`,
      driver: 'org.postgresql.Driver',
    }

    switch (cmd) {
      case 'diffChangeLog':
      case 'diff':
        options.referenceUrl = options.url
        options.referenceUsername = options.username
        options.referencePassword = options.password
        options.defaultSchemaName = this.options.migrations.schema!.default
        options.referenceDefaultSchemaName = this.options.migrations.schema!.reference
        break
      case 'update':
      case 'updateSQL':
      case 'dropAll':
      default:
        break
    }

    return options
  }

  async _synchronizeCloneDatabase(schema: string) {
    const { credentials } = this.options.service
    const cloneSchema = schema
    const temporaryChangelogFile = `${this.options.migrations.deploy.tmpFile}`

    const client = new Client(getCredentialsForClient(credentials))
    await client.connect()
    await client.query(`DROP SCHEMA IF EXISTS ${cloneSchema} CASCADE`)
    await client.query(`CREATE SCHEMA ${cloneSchema}`)
    await client.end()

    // Basically create a copy of the schema
    let options = this.liquibaseOptionsFor('diffChangeLog')
    options.defaultSchemaName = cloneSchema
    options.referenceDefaultSchemaName = this.options.migrations.schema!.default
    options.changeLogFile = temporaryChangelogFile

    await liquibase(options).run('diffChangeLog')

    // Remove unnecessary stuff
    const diffChangeLog = ChangeLog.fromFile(temporaryChangelogFile)
    _removePostgreSystemViewsFromChangelog(diffChangeLog)
    diffChangeLog.toFile(temporaryChangelogFile)

    // Now deploy the copy to the clone
    options = this.liquibaseOptionsFor('update')
    options.defaultSchemaName = cloneSchema
    options.changeLogFile = temporaryChangelogFile

    await liquibase(options).run('update')

    fs.unlinkSync(temporaryChangelogFile)

    return Promise.resolve()
  }

  /**
   * @override
   */
  async _deployCdsToReferenceDatabase() {
    const { credentials } = this.options.service
    const referenceSchema = this.options.migrations.schema!.reference
    const client = new Client(getCredentialsForClient(credentials))

    await client.connect()
    await client.query(`DROP SCHEMA IF EXISTS ${referenceSchema} CASCADE`)
    await client.query(`CREATE SCHEMA ${referenceSchema}`)
    await client.query(`SET search_path TO ${referenceSchema};`)

    const serviceInstance = cds.services[this.serviceKey] as PostgresDatabase

    const query = this.cdsSQL.map((q) => serviceInstance.cdssql2pgsql(q)).join(' ')

    await client.query(query)

    return client.end()
  }

  /**
   * @override
   */
  async _createDatabase() {
    const clientCredentials = getCredentialsForClient(this.options.service.credentials)
    const { database } = clientCredentials

    // Do not connect directly to the database
    delete clientCredentials.database
    const client = new Client(clientCredentials)

    await client.connect()
    try {
      // Revisit: should be more safe, but does not work
      // await client.query(`CREATE DATABASE $1`, [this.options.service.credentials.database])
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
  }
}
