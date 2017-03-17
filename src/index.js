import async from 'async';
import { backoff, chunkItems } from './helpers';

class BetterBatch {
    constructor(docClient, maxRetries = 10, backoffFunction = backoff) {
        this.docClient = docClient;
        this.maxRetries = maxRetries;
        this.backoffFunction = backoffFunction;
    }
    
    processChunk(chunk, operation, unprocessedCheck, callback) {
        let retryCount = 0;
        let shouldRetry = true;
        
        const allItems = new Map();

        async.whilst(() => shouldRetry, (callback) => {
            this.docClient[operation](chunk, (err, data) => {
                if (err) return callback(err);
                
                // Only used if this is a batchGet
                if (data.Responses) {
                    const tables = Object.keys(data.Responses);
                    
                    tables.forEach((table) => {
                        if (!allItems.has(table)) allItems.set(table, []);
                        
                        let itemsOnTable = allItems.get(table);
                        
                        itemsOnTable = itemsOnTable.concat(data.Responses[table]);
                        
                        allItems.set(table, itemsOnTable);
                    });
                }
                
                const unprocessed = unprocessedCheck(data);
                
                if (unprocessed) {
                    chunk.RequestItems = unprocessed;
                    if (retryCount < this.maxRetries) {
                        shouldRetry = true;
                        retryCount += 1;
                    } else {
                        shouldRetry = false;
                        return callback();
                    }
                    
                    return setTimeout(() => callback(), this.backoffFunction(retryCount));
                }
                
                shouldRetry = false;
                callback();
            });
        }, (err) => {
            // Note allItems is just an empty map if this is a write op, or the get returned no items
            callback(err, allItems);
        });
    }
    
    batchOperation(operation, chunks, unprocessedCheck, callback) {
        const allItems = new Map();
        
        async.eachSeries(chunks, (chunk, callback) => {
            this.processChunk(chunk, operation, unprocessedCheck, (err, itemMap) => {
                // Item map empty if it's a write or the get returned nothing
                itemMap.forEach((items, table) => {
                    if (!allItems.has(table)) allItems.set(table, []);
                    
                    let currentItems = allItems.get(table);
                    
                    currentItems = currentItems.concat(items);
                    
                    allItems.set(table, currentItems);
                });
                
                callback(err);
            });
        }, (err) => {
            const mapAsLiteral = {};
            
            allItems.forEach((items, table) => {
                mapAsLiteral[table] = items;
            });
            
            callback(err, mapAsLiteral);
        });
    }
    
    batchGet(params, callback) {
        const chunks = chunkItems(params, 100, true);
        
        this.batchOperation('batchGet', chunks, (data) => {
            if (Object.keys(data.UnprocessedKeys).length) {
                return data.UnprocessedKeys;
            }
        }, callback);
    }
    
    batchWrite(params, callback) {
        const chunks = chunkItems(params, 25);
        
        this.batchOperation('batchWrite', chunks, (data) => {
            if (Object.keys(data.UnprocessedItems).length) {
                return data.UnprocessedItems;
            }
        }, (err) => {
            // We don't just pass callback as a function argument because batchOperation
            // calls back with an error and an empty map if we're doing a write
            callback(err);
        });
    }
    
    query(params, callback) {
        let queryAgain = true;
        let items = [];
        
        async.whilst(() => queryAgain, (callback) => {
            this.docClient.query(params, (err, data) => {
                if (err) return callback(err);
                
                items = items.concat(data.Items);
                
                queryAgain = !!data.LastEvaluatedKey;
                params.ExclusiveStartKey = data.LastEvaluatedKey;

                callback();
            });
        }, (err) => {
            callback(err, items);
        });
    }
}

module.exports = BetterBatch;
