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
            connectionLimit: 10,
            host: init.host,
            user: init.user,
            password: init.pass,
            database: init.db,
            dateStrings: [
                'DATE',
                'DATETIME',
                'TIMESTAMP'
            ],
            multipleStatements: true
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
                return yield connection.query(query, args);
            }
            catch (err) {
                this.logger.error(err);
            }
            finally {
                yield connection.release();
            }
        });
    }
    query_single(query, args) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                return yield connection.query(query, args);
            }
            catch (err) {
                this.logger.error(err);
            }
            finally {
                yield connection.release();
            }
        });
    }
    insert(table_name, params) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                const columns = params.map(x => x.key).join(',');
                const ph = Array(params.length + 1).join('?').split('').join(',');
                const values = params.map(x => x.value !== null && x.value !== undefined ? x.value : null);
                const query = `INSERT INTO ${this.database_name}.${table_name} (${columns}) VALUES (${ph});`;
                const res = yield connection.query(query, values);
                if (res.serverStatus != 2 || res.affectedRows <= 0) {
                    this.logger.error(`Failed to execute insert query into ${table_name}`, this.database_name);
                    return { success: false };
                }
                const new_record_id = res.insertId;
                return { success: true, new_record_id };
            }
            catch (error) {
                this.logger.error(error);
                return { success: false };
            }
            finally {
                yield connection.release();
            }
        });
    }
    insert_many(table_name, columns_names, values_arr) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                const columns = columns_names.map(x => x).join(',');
                const values = values_arr.map(value => value !== null && value !== undefined ? value : 'NULL').join(',');
                const query = `INSERT INTO ${this.database_name}.${table_name} (${columns}) VALUES ${values};`;
                const res = yield connection.query(query, values);
                if (res.serverStatus != 2 || res.affectedRows <= 0) {
                    this.logger.error(`Failed to execute insert query into ${table_name}`, this.database_name);
                    return { success: false };
                }
                return { success: true };
            }
            catch (error) {
                this.logger.error(error);
                return { success: false };
            }
            finally {
                yield connection.release();
            }
        });
    }
    update(table_name, params, where) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                const values = params.map(x => x.value !== null && x.value !== undefined ? x.value : null);
                const set = params.map(x => `${x.key}=?`).join(',');
                const query = `UPDATE ${table_name} SET ${set} ${where ? 'WHERE ' + where : ''};`;
                const res = yield connection.query(query, values);
                if (res.serverStatus != 2) {
                    this.logger.error(`Failed to execute update query into ${table_name}`, this.database_name);
                    return { success: false };
                }
                return { success: true, affectedRows: res.affectedRows };
            }
            catch (error) {
                this.logger.error(error);
                return { success: false };
            }
            finally {
                yield connection.release();
            }
        });
    }
    select(table, columns = '*', where, limit, offset) {
        return __awaiter(this, void 0, void 0, function* () {
            let connection;
            try {
                connection = yield this.connection();
                const query = `SELECT ${columns} FROM ${this.database_name}.${table} ${where ? 'WHERE ' + where : ''} ${limit ? 'LIMIT ' + limit : ''} ${offset ? 'OFFSET ' + offset : ''}`.trim() + ';';
                const res = yield connection.query(query);
                if (!res || res.length <= 0)
                    return null;
                return res;
            }
            catch (error) {
                this.logger.error(error);
                return null;
            }
            finally {
                yield connection.release();
            }
        });
    }
}
exports.default = MySqlShell;
