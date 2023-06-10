import * as mysql from 'mysql2';

class MySqlShell {

    private logger: ILogger | undefined;
    private pool: mysql.Pool | undefined;
    private database_name: string | undefined;

    public on_init(init: IArgs, logger: ILogger) {

        this.logger = logger;
        this.database_name = init.db;
        this.pool = mysql.createPool({
            connectionLimit: init.connectionLimit,        // default 10
            host: init.host,
            user: init.user,
            password: init.pass,
            database: init.db,
            dateStrings: [
                'DATE',
                'DATETIME',
                'TIMESTAMP'
            ],
            multipleStatements: init.multipleStatements  // default false
        });
    }

    public on_destroy() {

        this.pool && this.pool.end();
    }

    private connection(): Promise<any> {

        if (!this.pool) throw new Error('MySQL pool is not initialized!');

        return new Promise((connResolve, connReject) => {

            this.pool!.getConnection((connErr, connection) => {

                if (connErr) connReject(connErr);

                const query = (sql: any, args: any) => {
                    return new Promise((resolve, reject) => {
                        connection.query(sql, args, (err, result) => {
                            if (err) reject(err);
                            resolve(result);
                        });
                    });
                };

                const release = () => {
                    return new Promise((resolve, reject) => {
                        try {
                            connection.release();
                            resolve(true);
                        } catch (error) {
                            reject(error);
                        }
                    });
                };

                connResolve({ query, release });

            });
        });
    };

    //  -------

    public async query<T>(query: string, args?: any[]): Promise<T[] | undefined> {

        let connection: any;

        try {

            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');

            return await connection.query(query, args);
        }
        catch (error:any) {

            this.log_error(error && error.message ? error.message : 'Failed to execute query');
        }
        finally {

            connection && await connection.release();
        }
    }

    public async query_single<T>(query: string, args?: any[]): Promise<T | undefined | IDeleteRes> {

        let connection: any;

        try {

            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');

            return await connection.query(query, args);
        }
        catch (error:any) {

            this.log_error(error && error.message ? error.message : 'Failed to execute query');
        }
        finally {

            connection && await connection.release();
        }
    }

    //  -------

    public async select<T>(table_name: string, columns: string = '*', where?: { [key: string]: any }, limit?: number, offset?: number) : Promise<T[] | null> {

        let connection: any;

        try {

            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');

            let whereClause = '';
            let whereValues: any[] = [];
            if (where) {
                const whereParts = Object.entries(where).map(([key, value], i) => {
                    whereValues.push(value);
                    return `${key} = ?`;
                });
                whereClause = 'WHERE ' + whereParts.join(' AND ');
            }

            const query = `SELECT ${columns} FROM ${this.database_name}.${table_name} ${whereClause} ${limit ? 'LIMIT ?' : ''} ${offset ? 'OFFSET ?' : ''}`.trim() + ';';
            const values = [...whereValues, limit, offset].filter(x => x !== undefined);

            return await connection.query(query, values);

        } catch (error: any) {

            this.log_error(error && error.message ? error.message : 'Failed to execute select query');
            return null;

        } finally {

            connection && await connection.release();
        }
    }

    //  -------

    public async insert(table_name: string, params: { key: string, value: any }[]): Promise<IInsertRes> {

        let connection: any;
        let res: any;
    
        try {

            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');
    
            const columns = params.map(x => x.key).join(',');
            const placeholders = params.map(() => '?').join(',');
            const values = params.map(x => x.value);
    
            const query = `INSERT INTO ${this.database_name}.${table_name} (${columns}) VALUES (${placeholders});`;
    
            res = await connection.query(query, values);
    
            const new_record_id = res.insertId as number;
            return { success: true, new_record_id };
    
        } catch (error:any) {

            const errorMessage = error && error.message ? error.message : 'Failed to execute insert query';
            this.log_error(errorMessage);
            return { success: false, errorMessage, ...res };

        } finally {

            connection && await connection.release();
        }
    }
    
    public async insert_many(table_name: string, columns_names: string[], values_arr: any[][]): Promise<IInsertRes> {

        let connection: any;
        let res: any;

        try {

            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');

            if (!Array.isArray(values_arr)) throw new Error('Values should be an array of arrays');

            const columns = columns_names.join(',');

            const placeholders = values_arr.map(
                () => `(${columns_names.map(() => '?').join(',')})`
            ).join(',');

            const flattenedValues = values_arr.reduce((acc, val) => acc.concat(val), []);

            const query = `INSERT INTO ${this.database_name}.${table_name} (${columns}) VALUES ${placeholders};`

            res = await connection.query(query, flattenedValues);

            return { success: true };

        } catch (error: any) {

            const errorMessage = error && error.message ? error.message : 'Failed to execute insert query';
            this.log_error(errorMessage);
            return { success: false, errorMessage, ...res };

        } finally {

            connection && await connection.release();
        }
    }

    //  -------

    public async update(table_name: string, params: { key: string, value: any }[], where: { [key: string]: any }) : Promise<IUpdateRes> {

        let connection: any;
        let res: any;

        try {

            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');

            const values = params.map(x => x.value !== null && x.value !== undefined ? x.value : null);
            const set = params.map(x => `${x.key}=?`).join(',');

            let whereClause = '';
            let whereValues: any[] = [];
            if (where) {
                const whereParts = Object.entries(where).map(([key, value], i) => {
                    whereValues.push(value);
                    return `${key} = ?`;
                });
                whereClause = 'WHERE ' + whereParts.join(' AND ');
            }

            const query = `UPDATE ${this.database_name}.${table_name} SET ${set} ${whereClause};`;
            const queryParams = [...values, ...whereValues];

            res = await connection.query(query, queryParams);

            return { success: true, affectedRows: res.affectedRows };

        } catch (error:any) {

            const errorMessage = error && error.message ? error.message : 'Failed to execute update query';
            this.log_error(errorMessage);
            return { success: false, errorMessage, ...res };

        } finally {

            connection && await connection.release();
        }
    }

    public async update_many(table_name: string, params: {key: string, value: any, where:{[key:string]:any}}[], abortOnFail:boolean=false) : Promise<IUpdateRes> {

        let connection: any;
        let res: any;
    
        try {

            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');
    
            let affectedRows = 0;

            for (const row of params) {

                const set = `${row.key}=?`;
    
                let whereClause = '';
                let whereValues: any[] = [];
                if (row.where) {
                    const whereParts = Object.entries(row.where).map(([key, value]) => {
                        whereValues.push(value);
                        return `${key} = ?`;
                    });
                    whereClause = 'WHERE ' + whereParts.join(' AND ');
                }
    
                const query = `UPDATE ${this.database_name}.${table_name} SET ${set} ${whereClause};`;
                const queryParams = [row.value, ...whereValues];
    
                res = await connection.query(query, queryParams);

                affectedRows += (res && res.affectedRows ? res.affectedRows : 0);
            }
    
            return { success: true, affectedRows };
    
        } catch (error:any) {

            const errorMessage = error && error.message ? error.message : 'Failed to execute update_many query';
            this.log_error(errorMessage)
            return { success: false, errorMessage, ...res };

        } finally {

            connection && await connection.release();
        }
    }

    //  -------

    public async delete(table_name: string, where:{[key:string]:any}, whereIn:{[key:string]:any[]} = {}) : Promise<IDeleteRes> {

        let connection: any;
        let res: any;
    
        try {
            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');
    
            let whereClause = '';
            let whereValues: any[] = [];
            if (where) {
                const whereParts = Object.entries(where).map(([key, value]) => {
                    whereValues.push(value);
                    return `${key} = ?`;
                });
                whereClause = 'WHERE ' + whereParts.join(' AND ');
            }
    
            const whereInParts = Object.entries(whereIn).map(([key, values]) => {
                whereValues.push(...values);
                const placeholders = values.map(() => '?').join(', ');
                return `${key} IN (${placeholders})`;
            });
            
            if (whereInParts.length) {
                whereClause = whereClause ? whereClause + ' AND ' + whereInParts.join(' AND ') : 'WHERE ' + whereInParts.join(' AND ');
            }
    
            const query = `DELETE FROM ${this.database_name}.${table_name} ${whereClause};`;
            const queryParams = [...whereValues];
    
            res = await connection.query(query, queryParams);
    
            return { success: true, ...res };
    
        } catch (error:any) {
    
            const errorMessage = error && error.message ? error.message : 'Failed to execute delete query';
            this.log_error(errorMessage);
            return { success: false, errorMessage, ...res };
    
        } finally {
    
            connection && await connection.release();
        }
    }
    
    public async delete_many(table_name: string, wheres: {[key:string]:any}[]) : Promise<IDeleteRes> {

        let connection: any;
        let res: any;
    
        try {

            connection = await this.connection();
            if (!connection) throw new Error('Failed to get connection');
    
            let affectedRows = 0;

            for (const where of wheres) {

                let whereClause = '';
                let whereValues: any[] = [];
                if (where) {
                    const whereParts = Object.entries(where).map(([key, value]) => {
                        whereValues.push(value);
                        return `${key} = ?`;
                    });
                    whereClause = 'WHERE ' + whereParts.join(' AND ');
                }
    
                const query = `DELETE FROM ${this.database_name}.${table_name} ${whereClause};`;
                const queryParams = [...whereValues];
    
                res = await connection.query(query, queryParams);

                affectedRows += (res && res.affectedRows ? res.affectedRows : 0);
            }
    
            return { success: true, affectedRows };
    
        } catch (error:any) {

            const errorMessage = error && error.message ? error.message : 'Failed to execute delete_many query';
            this.log_error(errorMessage)
            return { success: false, errorMessage, ...res };

        } finally {

            connection && await connection.release();
        }
    }

    //  -------

    private log_error(message:string, args?:any) {
        this.logger && this.logger.error(message, [this.database_name, ...args]);
    }
}

interface ILogger {
    error(message: any, args?: any): void;
}

interface IArgs {

    host: string
    user: string
    pass: string
    db: string
    connectionLimit?: number
    multipleStatements?: boolean
}

interface IInsertRes {

    success: boolean
    message?: string
    errorMessage?: string
    new_record_id?: number
}

interface IUpdateRes {

    success: boolean
    message? : string
    errorMessage?: string
    affectedRows?: number
}

interface IDeleteRes {

    success: boolean
    message? : string
    errorMessage?: string
    affectedRows?: number

    // fieldCount: number
    // info: string
    // insertId: number
    // serverStatus: number
    // warningStatus: number
}


export default MySqlShell;