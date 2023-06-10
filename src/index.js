"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const mysql = __importStar(require("mysql2"));
class MySqlShell {
    on_init(init, logger) {
        this.logger = logger;
        this.database_name = init.db;
        this.pool = mysql.createPool({
            connectionLimit: init.connectionLimit,
            host: init.host,
            user: init.user,
            password: init.pass,
            database: init.db,
            dateStrings: [
                'DATE',
                'DATETIME',
                'TIMESTAMP'
            ],
            multipleStatements: init.multipleStatements // default false
        });
    }
    on_destroy() {
        this.pool && this.pool.end();
    }
    connection() {
        if (!this.pool)
            throw new Error('MySQL pool is not initialized!');
        return new Promise((connResolve, connReject) => {
            this.pool.getConnection((connErr, connection) => {
                if (connErr)
                    connReject(connErr);
                const query = (sql, args) => {
                    return new Promise((resolve, reject) => {
                        connection.query(sql, args, (err, result) => {
                            if (err)
                                reject(err);
                            resolve(result);
                        });
                    });
                };
                const release = () => {
                    return new Promise((resolve, reject) => {
                        try {
                            connection.release();
                            resolve(true);
                        }
                        catch (error) {
                            reject(error);
                        }
                    });
                };
                connResolve({ query, release });
            });
        });
    }
    ;
    query(query, args) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                return yield connection.query(query, args);
            }
            catch (error) {
                this.log_error(error && error.message ? error.message : 'Failed to execute query');
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    query_single(query, args) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                return yield connection.query(query, args);
            }
            catch (error) {
                this.log_error(error && error.message ? error.message : 'Failed to execute query');
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    insert(table_name, params) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                const columns = params.map(x => x.key).join(',');
                const placeholders = params.map(() => '?').join(',');
                const values = params.map(x => x.value);
                const query = `INSERT INTO ${this.database_name}.${table_name} (${columns}) VALUES (${placeholders});`;
                const res = yield connection.query(query, values);
                if (res.serverStatus != 2) {
                    this.log_error(`Failed to execute insert query into ${table_name}`, [res.message]);
                    return { success: false, message: res.message };
                }
                const new_record_id = res.insertId;
                return { success: true, new_record_id };
            }
            catch (error) {
                const message = error && error.message ? error.message : 'Failed to execute insert query';
                this.log_error(message);
                return { success: false, message };
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    insert_many(table_name, columns_names, values_arr) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                if (!Array.isArray(values_arr))
                    throw new Error('Values should be an array of arrays');
                const columns = columns_names.join(',');
                const placeholders = values_arr.map(() => `(${columns_names.map(() => '?').join(',')})`).join(',');
                const flattenedValues = values_arr.reduce((acc, val) => acc.concat(val), []);
                const query = `INSERT INTO ${this.database_name}.${table_name} (${columns}) VALUES ${placeholders};`;
                const res = yield connection.query(query, flattenedValues);
                if (res.serverStatus != 2) {
                    this.log_error(`Failed to execute insert query into ${table_name}`, [res.message]);
                    return { success: false, message: res.message };
                }
                return { success: true };
            }
            catch (error) {
                const message = error && error.message ? error.message : 'Failed to execute insert query';
                this.log_error(message);
                return { success: false, message };
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    update(table_name, params, where) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                const values = params.map(x => x.value !== null && x.value !== undefined ? x.value : null);
                const set = params.map(x => `${x.key}=?`).join(',');
                let whereClause = '';
                let whereValues = [];
                if (where) {
                    const whereParts = Object.entries(where).map(([key, value], i) => {
                        whereValues.push(value);
                        return `${key} = ?`;
                    });
                    whereClause = 'WHERE ' + whereParts.join(' AND ');
                }
                const query = `UPDATE ${this.database_name}.${table_name} SET ${set} ${whereClause};`;
                const queryParams = [...values, ...whereValues];
                const res = yield connection.query(query, queryParams);
                if (res.serverStatus != 2) {
                    this.log_error(`Failed to execute update query into ${table_name}`, [res.message]);
                    return { success: false, affectedRows: res.affectedRows, message: res.message };
                }
                return { success: true, affectedRows: res.affectedRows };
            }
            catch (error) {
                const message = error && error.message ? error.message : 'Failed to execute update query';
                this.log_error(message);
                return { success: false, message };
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    update_many(table_name, params, abortOnFail = false) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                let affectedRows = 0;
                for (const row of params) {
                    const set = `${row.key}=?`;
                    let whereClause = '';
                    let whereValues = [];
                    if (row.where) {
                        const whereParts = Object.entries(row.where).map(([key, value]) => {
                            whereValues.push(value);
                            return `${key} = ?`;
                        });
                        whereClause = 'WHERE ' + whereParts.join(' AND ');
                    }
                    const query = `UPDATE ${this.database_name}.${table_name} SET ${set} ${whereClause};`;
                    const queryParams = [row.value, ...whereValues];
                    const res = yield connection.query(query, queryParams);
                    if (!res || res.serverStatus != 2) {
                        this.log_error(`Failed to execute update_many into ${table_name}`, [res.message]);
                        if (abortOnFail)
                            return { success: false, affectedRows, message: res.message };
                    }
                    affectedRows += (res && res.affectedRows ? res.affectedRows : 0);
                }
                return { success: true, affectedRows };
            }
            catch (error) {
                const message = error && error.message ? error.message : 'Failed to execute update_many query';
                this.log_error(message);
                return { success: false, message };
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    select(table_name, columns = '*', where, limit, offset) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                let whereClause = '';
                let whereValues = [];
                if (where) {
                    const whereParts = Object.entries(where).map(([key, value], i) => {
                        whereValues.push(value);
                        return `${key} = ?`;
                    });
                    whereClause = 'WHERE ' + whereParts.join(' AND ');
                }
                const query = `SELECT ${columns} FROM ${this.database_name}.${table_name} ${whereClause} ${limit ? 'LIMIT ?' : ''} ${offset ? 'OFFSET ?' : ''}`.trim() + ';';
                const values = [...whereValues, limit, offset].filter(x => x !== undefined);
                const res = yield connection.query(query, values);
                if (res.serverStatus != 2)
                    this.log_error(`Failed to execute select query into ${table_name}`, [res.message]);
                return !res || res.length <= 0 ? null : res;
            }
            catch (error) {
                this.log_error(error && error.message ? error.message : 'Failed to execute select query');
                return null;
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    delete(table_name, where) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                let whereClause = '';
                let whereValues = [];
                if (where) {
                    const whereParts = Object.entries(where).map(([key, value]) => {
                        whereValues.push(value);
                        return `${key} = ?`;
                    });
                    whereClause = 'WHERE ' + whereParts.join(' AND ');
                }
                const query = `DELETE FROM ${this.database_name}.${table_name} ${whereClause};`;
                const queryParams = [...whereValues];
                const res = yield connection.query(query, queryParams);
                if (res.serverStatus != 2 || res.affectedRows <= 0) {
                    this.log_error(`Failed to execute delete query into ${table_name}`, [res.message]);
                    return { success: false, affectedRows: res.affectedRows, message: res.message };
                }
                return { success: true, affectedRows: res.affectedRows };
            }
            catch (error) {
                const message = error && error.message ? error.message : 'Failed to execute delete query';
                this.log_error(message);
                return { success: false, message };
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    delete_many(table_name, wheres, abortOnFail = false) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                if (!connection)
                    throw new Error('Failed to get connection');
                let affectedRows = 0;
                for (const where of wheres) {
                    let whereClause = '';
                    let whereValues = [];
                    if (where) {
                        const whereParts = Object.entries(where).map(([key, value]) => {
                            whereValues.push(value);
                            return `${key} = ?`;
                        });
                        whereClause = 'WHERE ' + whereParts.join(' AND ');
                    }
                    const query = `DELETE FROM ${this.database_name}.${table_name} ${whereClause};`;
                    const queryParams = [...whereValues];
                    const res = yield connection.query(query, queryParams);
                    if (res.serverStatus != 2 || res.affectedRows <= 0) {
                        this.log_error(`Failed to execute delete_many into ${table_name}`, [res.message]);
                        if (abortOnFail)
                            return { success: false, affectedRows, message: res.message };
                    }
                    affectedRows += (res && res.affectedRows ? res.affectedRows : 0);
                }
                return { success: true, affectedRows };
            }
            catch (error) {
                const message = error && error.message ? error.message : 'Failed to execute delete_many query';
                this.log_error(message);
                return { success: false, message };
            }
            finally {
                connection && (yield connection.release());
            }
        });
    }
    log_error(message, args) {
        this.logger && this.logger.error(message, [this.database_name, ...args]);
    }
}
exports.default = MySqlShell;
