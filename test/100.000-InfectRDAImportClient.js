import InfectRDAImportClient from '../src/InfectRDAImportClient.js';
import section from 'section-tests';
import assert from 'assert';
import ServiceManager from '@infect/rda-service-manager';
import RainbowConfig from '@rainbow-industries/rainbow-config';
import RegistryClient from '@infect/rda-service-registry-client';
import path from 'path';
import { AnresisTestData } from '@infect/rda-fixtures';



section('Infect RDA Import Client', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=* --data-for-dev'.split(' ')
        });
        
        await sm.startServices('@infect/rda-service-registry');
        await sm.startServices('@infect/infect-rda-sample-importer');
        await sm.startServices('@infect/infect-rda-sample-storage');
        await sm.startServices('@infect/api');
    });



    section.test('create client, create import, import rows, commit', async() => {
        section.setTimeout(20000);

        const configDir = path.join(path.dirname(new URL(import.meta.url).pathname), '../config/');
        const config = new RainbowConfig(configDir, path.join(configDir, '../'));
        await config.load();

        const registryClient = new RegistryClient(config.get('service-registry.host')); 

        const testData = new AnresisTestData();
        const rows = await testData.getData();


        section.info('set up client');
        const client = new InfectRDAImportClient({
            registryClient,
        });


        section.info('create import');
        await client.createImport({
            dataSetIdentifier: `client-${Math.random()}`,
            dataVersionIdentifier: `client-${Math.random()}`,
            dataVersionDescription: `client-${Math.random()}`,
        });


        section.info('import rows');
        await client.storeSamples(rows);



        section.info('commit');
        await client.commit();

        await client.end();
    });


    section.destroy(async() => {
        await sm.stopServices();
    });
});