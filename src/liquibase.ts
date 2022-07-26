import childProcess from 'child_process'
import path from 'path'
import { liquibaseOptions } from './config'

function transformObjectToCmdString(params) {
  return Object.entries(params)
    .filter(([key]) => key !== 'liquibase')
    .map(([key, value]) => `--${key}=${value}`)
    .join(' ')
}

class Liquibase {
  params: liquibaseOptions

  /**
   * Returns an instance of a lightweight Liquibase Wrapper.
   */
  constructor(params = {}) {
    const defaultParams = {
      liquibase: path.join(__dirname, './../liquibase/liquibase'), // Liquibase executable
      classpath: `${__dirname}/../drivers/postgresql-42.3.2.jar`,
      driver: 'org.postgresql.Driver',
    }
    this.params = { ...params, ...defaultParams }
  }

  /**
   * Executes a Liquibase command.
   *
   * @param {string} action a string for the Liquibase command to run. Defaults to `'update'`
   * @param {string} params any parameters for the command
   * @returns {Promise} Promise of a node child process.
   */
  public run(action = 'update', params = {}) {
    return this.exec(`${this.command} ${transformObjectToCmdString(params)} ${action}`)
  }

  /**
   * Internal getter that returns a node child process compatible command string.
   *
   * @returns {string}
   * @private
   */
  get command() {
    const cmd = `${path.join(__dirname, './../liquibase/liquibase')} ${transformObjectToCmdString(this.params)}`

    return cmd
  }

  /**
   *
   * Internal method for executing a child process.
   *
   * @param {string} command Liquibase command
   * @param {*} options any options
   * @private
   * @returns {Promise} Promise of a node child process.
   */

  // eslint-disable-next-line class-methods-use-this
  private exec(command: string, options = {}) {
    // console.warn(command)
    return new Promise((resolve, reject) => {
      childProcess.exec(command, options, (error: any, stdout: any) => {
        if (error) {
          // console.log('\n', stdout)
          // console.error('\n', stderr)
          // Log things in case of an error
          // eslint-disable-next-line no-param-reassign
          error.stderr = stdout
          return reject(error)
        }

        return resolve({
          stdout,
        })
      })
    })
  }
}

function LiquibaseGenerator(params: liquibaseOptions) {
  return new Liquibase(params)
}

export default LiquibaseGenerator
