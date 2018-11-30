const rq = require('request-promise');
const _ = require('lodash');
const minimist = require('minimist');
const url = require('url').URL;
const winston = require('./winston');

const csv2json = require("csvtojson");

const args = minimist(process.argv.slice(2));


const username = args['dhis2-username'] || 'Jeric';
const password = args['dhis2-password'] || '20SeraPkp8FA!18';
const dhisUrl = args['dhis2-url'] || 'http://localhost:8080';

const serverUrl = 'http://10.10.82.196/AQUARIUS/Publish/AquariusPublishRestService.svc/';


const dhis2 = new url(dhisUrl);

dhis2.username = username;
dhis2.password = password;

const baseUrl = dhis2.toString() + 'api/';

const DATA_URL = baseUrl + 'dataValueSets';


const dataSets = require('./dataMapping.json');
const ouMappings = require('./orgUnitMapping.json');

const nest = (seq, keys) => {
    if (!keys.length)
        return seq;
    const first = keys[0];
    const rest = keys.slice(1);
    return _.mapValues(_.groupBy(seq, first), (value) => {
        return nest(value, rest)
    });
};


const login = async () => {
    const url = serverUrl + 'GetAuthToken';
    const params = {user: 'rbme', encPwd: 'D@ta!'};
    const response = await rq({uri: url, qs: params, encoding: null});
    return response.toString('utf8');
};


const downloadData = async (endpoint, params) => {
    const token = await login();
    const url = serverUrl + endpoint;
    params = {...params, token};
    try {
        const response = await rq({uri: url, qs: params, encoding: null});
        const responseString = response.toString('utf8');
        return await csv2json().fromString(responseString)
    } catch (error) {
        winston.log({level: 'warn', message: 'Something wired happened'});
    }
};


const processWaterData = async dataElements => {
    let data = await downloadData('GetDataSetsList', {});

    let processed = [];

    dataElements.forEach(de => {
        const found = data.filter(d => {
            return d['Parameter'] === de.param;
        });
        const realData = found.map(d => {
            const val = {};
            val['Parameter'] = de.mapping.value;
            val['Category'] = 'default';
            val['Location'] = ouMappings[d['LocationId']];
            val['Value'] = d['EndValue']; //Computations might be applied here in case Timestamps data is used
            val['Year'] = '2018July'; //Finanancial
            return val;
        }).filter(d => {
            return d.Location;
        });
        processed = [...processed, ...realData];

    });
    return processed;
};


const insertData = data => {
    console.log(DATA_URL);
    const options = {
        method: 'POST',
        uri: DATA_URL,
        body: data,
        json: true
    };
    return rq(options);
};


const processData = (dataSet, data) => {
    const forms = dataSet.forms;
    let dataValues = [];

    data = nest(data, [dataSet.dataElementColumn.value]);
    const dataSetUnits = _.fromPairs(dataSet.organisationUnits.map(o => {
        if (dataSet.orgUnitStrategy.value === 'name') {
            return [o.name, o.id];
        } else if (dataSet.orgUnitStrategy.value === 'code') {
            return [o.code, o.id];
        }
        return [o.id, o.id];
    }));

    forms.forEach(f => {
        let p = {};
        f.dataElements.forEach(element => {
            if (element.mapping) {
                const foundData = data[element.mapping.value];
                // console.log(foundData);
                let groupedData = {};
                if (foundData) {
                    groupedData = _.fromPairs(foundData.map(d => {
                        return [d[dataSet.categoryOptionComboColumn.value], {
                            period: d[dataSet.periodColumn.value],
                            value: d[dataSet.dataValueColumn.value],
                            orgUnit: d[dataSet.orgUnitColumn.value]
                        }]
                    }));

                    const obj = _.fromPairs([[element.id, groupedData]]);
                    p = {...p, ...obj}
                }
            }
        });
        data = p;
        if (data) {
            f.categoryOptionCombos.forEach(coc => {
                _.forOwn(coc.mapping, (mapping, dataElement) => {
                    // console.log(dataElement);
                    if (data[dataElement]) {
                        const orgUnit = dataSetUnits[data[dataElement][mapping.value]['orgUnit']];
                        console.log(data[dataElement][mapping.value]);
                        if (orgUnit) {
                            dataValues = [...dataValues, {
                                dataElement,
                                value: data[dataElement][mapping.value]['value'],
                                period: data[dataElement][mapping.value]['period'],
                                categoryOptionCombo: coc.id,
                                orgUnit
                            }]
                        }
                    }
                })
            });
        }
    });

    return dataValues;

};

dataSets.forEach(async dataSet => {
    //Otm1usl7iVh WATER FORM
    //bU2LoHFGUzr
    const id = dataSet.id;
    let data;
    if (id === 'bU2LoHFGUzr') {//National Monthly
        data = require('./data_samples/lands_national_monthly.json');
    } else if (id === 'EJMcDUrnwIZ') { //District Quarterly
        data = require('./data_samples/lands_district_quaterly.json');
    } else if (id === 'Otm1usl7iVh') {
        const form = dataSet.forms[0];
        const dataElements = form.dataElements.filter(de => {
            return de.param
        });
        data = await processWaterData(dataElements);
    }

    const dataValues = processData(dataSet, data);

    try {
        const response = await insertData({dataValues});
        console.log(response);
    } catch (e) {
        console.log(e);
    }
});



