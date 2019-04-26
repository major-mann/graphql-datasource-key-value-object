module.exports = createKeyValueSource;

const TYPE_NAME = 'KeyValue';

const uuid = require('uuid');
const createGraphqlSource = require('@major-mann/graphql-datasource-base');

async function createKeyValueSource({ object, typeName = TYPE_NAME }) {
    if (!object || typeof object !== 'object') {
        object = Object.create(null);
    }

    typeName = typeName || TYPE_NAME;
    const composer = await createGraphqlSource({
        data,
        definitions: `
            type ${typeName} {
                key: ID!
                value: String!
            }
        `,
        rootTypes: [typeName]
    });

    const mutationType = composer.getOTC(`${typeName}Mutation`);
    mutationType.removeField('upsert');
    mutationType.removeField('update');
    wrapResolver(mutationType, 'create', createWrapper);

    return composer;

    function wrapResolver(type, name, wrapper) {
        const resolverName = `$${name}`;
        type.setResolver(resolverName, type.getResolver(resolverName).wrap(wrapper));
        type.setField(name, type.getResolver(resolverName));
    }

    function createWrapper(resolver) {
        resolver.setArgs({
            key: 'ID!',
            value: 'String!'
        });
        return resolver.wrapResolve(next => params => {
            return next({
                ...params,
                args: {
                    key: params.args.key,
                    data: {
                        value: params.args.value
                    }
                }
            });
        });
    }

    function data() {
        return {
            find,
            list,
            create,
            upsert,
            update,
            delete: remove
        };

        function find(id) {
            return {
                key: id,
                value: object[id]
            };
        }

        function create(id, data) {
            id = id || uuid.v4();
            if (object[id]) {
                throw new Error(`Entry with id "${id}" already exists`);
            }
            object[id] = String(data.value);
            return id;
        }

        function upsert(id, data) {
            object[id] = String(data.value);
            return id;
        }

        function update(id, data) {
            if (!object[id]) {
                throw new Error(`Entry with id "${id}" does not exist`);
            }
            object[id] = String(data.value);
            return id;
        }

        function remove(id) {
            delete object[id];
        }

        function list({ filter, order, first, last, before, after }) {
            let data = Object.keys(object).map(id => ({
                key: id,
                value: object[id]
            }));
            if (filter) {
                filter.forEach(flt => data = data.filter(applyFilter(flt)));
            }
            if (order) {
                order.reverse();
                order.forEach(ord => data.sort(applyOrder(ord)));
            }
            let hasPreviousPage, hasNextPage;
            if (after && data.length) {
                hasPreviousPage = true;
                trimmer(() => data.shift());
            }
            if (before && data.length) {
                hasNextPage = true;
                trimmer(() => data.pop());
            }

            if (first && last) {
                if (first > last) {
                    sliceStart();
                    sliceEnd();
                } else if (last > first) {
                    sliceEnd();
                    sliceStart();
                } else {
                    sliceStart();
                }
            } else if (first) {
                sliceStart();
            } else if (last) {
                sliceEnd();
            }

            return {
                edges: data.map(createEdge),
                pageInfo: {
                    hasPreviousPage,
                    hasNextPage
                }
            };

            function createEdge(node) {
                return {
                    node,
                    cursor: createCursor(node)
                };
            }

            function createCursor(doc) {
                let data;
                if (order && order.length) {
                    data = order.map(ord => String(doc[ord.field]));
                } else {
                    data = doc.key;
                }
                return serializeCursor(data);
            }

            function trimmer(remove) {
                let current;
                do {
                    current = remove();
                } while (data.length && !cursorMatches(current));
            }

            function cursorMatches(doc, cursor) {
                cursor = deserializeCursor(cursor);
                if (!order || !order.length) {
                    return doc.key === cursor;
                } else if (order && Array.isArray(cursor) && order.length === cursor.length) {
                    return cursor.every((val, idx) => doc[order[idx].field] === val);
                } else {
                    return false;
                }
            }

            function serializeCursor(data) {
                const source = JSON.stringify(data);
                return Buffer.from(source).toString('base64');
            }

            function deserializeCursor(opaque) {
                if (opaque) {
                    const source = Buffer.from(opaque, 'base64');
                    const json = JSON.parse(source);
                    return json;
                } else {
                    return undefined;
                }
            }

            function sliceStart() {
                if (first === data.length) {
                    return;
                }
                data = data.slice(0, first);
                hasNextPage = true;
            }

            function sliceEnd() {
                if (last === data.length) {
                    return;
                }
                data = data.slice(data.length - last, data.length);
                hasPreviousPage = true;
            }

            function applyFilter(flt) {
                return function check(element) {
                    switch (flt.op) {
                        case 'LT':
                            return element[flt.field] < flt.value;
                        case 'LTE':
                        return element[flt.field] <= flt.value;
                        case 'EQ':
                            // Double equals on purpose
                            return element[flt.field] == flt.value;
                        case 'GTE':
                            return element[flt.field] >= flt.value;
                        case 'GT':
                            return element[flt.field] > flt.value;
                        case 'CONTAINS':
                            return Array.isArray(element[flt.field]) && element[flt.field].includes(flt.value) ||
                                element[flt.field] == flt.value;
                        default:
                            throw new Error(`Unrecognized filter operation "${filter.op}"`);
                    }
                };
            }

            function applyOrder(ord) {
                return function sort(a ,b) {
                    if (ord.desc) {
                        const tmp = a;
                        a = b;
                        b = tmp;
                    }
                    const strA = String(a[ord.field]);
                    const strB = String(b[ord.field]);
                    if (strA < strB) {
                        return -1;
                    } else if (strA > strB) {
                        return 1;
                    } else {
                        return 0;
                    }
                };
            }
        }
    }
}