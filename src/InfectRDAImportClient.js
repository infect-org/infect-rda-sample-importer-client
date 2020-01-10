import HTTP2Client from '@distributed-systems/http2-client';



export default class InfectRDAImportClient {
    
    constructor({
        registryClient,
    }) {
        this.registryClient = registryClient;
        this.httpClient = new HTTP2Client();
    }



    async createImport({
        dataSetIdentifier,
        dataVersionIdentifier,
        dataVersionDescription,
    }) {
        const importHost = await this.registryClient.resolve('infect-rda-sample-import');
        const response = await this.httpClient.post(`${importHost}/infect-rda-sample-import.import`).expect(201).send({
            dataSetIdentifier,
            dataVersionIdentifier,
            dataVersionDescription,
            importProcessorName: `anresis-human`,
        });

        const { id } = await response.getData();
        this.importId = id;
    }



    async commit() {
        const importHost = await this.registryClient.resolve('infect-rda-sample-import');
        await this.httpClient.patch(`${importHost}/infect-rda-sample-import.import/${this.importId}`).expect(200).send();
    }



    async delete() {
        const importHost = await this.registryClient.resolve('infect-rda-sample-import');
        await this.httpClient.delete(`${importHost}/infect-rda-sample-import.import/${this.importId}`).expect(200).send();
    }



    async storeSamples(rows) {
        const importHost = await this.registryClient.resolve('infect-rda-sample-import');
        const dataResponse = await this.httpClient.post(`${importHost}/infect-rda-sample-import.data`).expect(201).query({
            'return-invalid-data': true,
        }).send({
            id: this.importId,
            records: rows,
        });

        const invalidSamples = await dataResponse.getData();
        return invalidSamples;
    }


    async end() {
        await this.httpClient.end();
    }
}
