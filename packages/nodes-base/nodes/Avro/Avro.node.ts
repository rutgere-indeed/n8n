import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IDataObject,
} from 'n8n-workflow';
import { NodeOperationError, NodeConnectionTypes } from 'n8n-workflow';

export class Avro implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Avro',
		name: 'avro',
		icon: 'file:avro.svg',
		group: ['transform'],
		version: 1,
		description: 'Deserialize Avro data using Schema Registry',
		defaults: {
			name: 'Avro',
		},
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		credentials: [
			{
				name: 'schemaRegistryApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Deserialize',
						value: 'deserialize',
						description: 'Deserialize Avro encoded data',
						action: 'Deserialize Avro data',
					},
				],
				default: 'deserialize',
			},
			{
				displayName: 'Input Data Format',
				name: 'inputDataFormat',
				type: 'options',
				displayOptions: {
					show: {
						operation: ['deserialize'],
					},
				},
				options: [
					{
						name: 'Auto-Detect (Binary)',
						value: 'auto',
						description: 'Automatically detect and use binary data from input',
					},
					{
						name: 'Binary',
						value: 'binaryProperty',
						description: 'Read Avro data from a specific binary property',
					},
					{
						name: 'Base64',
						value: 'base64String',
						description: 'Read Avro data from a base64 encoded field',
					},
					{
						name: 'Hex',
						value: 'hexString',
						description: 'Read Avro data from a hex encoded field',
					},
				],
				default: 'auto',
				description: 'The format of the input data',
			},
			{
				displayName: 'Binary Property',
				name: 'binaryPropertyName',
				type: 'string',
				default: 'data',
				required: true,
				requiresDataPath: 'single',
				displayOptions: {
					show: {
						operation: ['deserialize'],
						inputDataFormat: ['binaryProperty'],
					},
				},
				placeholder: 'data',
				hint: 'The name of the input binary field containing the Avro data',
				description: 'Name of the binary property containing Avro data',
			},
			{
				displayName: 'Encoded Data',
				name: 'encodedData',
				type: 'string',
				default: '',
				displayOptions: {
					show: {
						operation: ['deserialize'],
						inputDataFormat: ['base64String', 'hexString'],
					},
				},
				placeholder: 'Add an expression to reference data, e.g. {{ $json.myField }}',
				description:
					'The encoded Avro data. Drag and drop a field from the left or use an expression.',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Output Property Name',
						name: 'outputPropertyName',
						type: 'string',
						default: 'data',
						requiresDataPath: 'single',
						placeholder: 'e.g. data',
						hint: 'Leave empty to replace the entire item with deserialized data',
						description:
							'Name of the property to store deserialized data in. Leave empty to replace the entire item.',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;

		try {
			const credentials = await this.getCredentials('schemaRegistryApi');

			const registryConfig: {
				host: string;
				auth?: { username: string; password: string };
			} = {
				host: credentials.url as string,
			};

			// Add authentication if configured
			if (credentials.authentication === 'basicAuth') {
				if (credentials.username && credentials.password) {
					registryConfig.auth = {
						username: credentials.username as string,
						password: credentials.password as string,
					};
				}
			}

			const registry = new SchemaRegistry(registryConfig);

			if (operation === 'deserialize') {
				const inputDataFormat = this.getNodeParameter('inputDataFormat', 0) as string;
				const options = this.getNodeParameter('options', 0, {}) as IDataObject;
				const outputPropertyName = options.outputPropertyName as string | undefined;

				for (let i = 0; i < items.length; i++) {
					try {
						let avroData: Buffer;

						// Get the Avro data based on input format
						if (inputDataFormat === 'auto' || inputDataFormat === 'binaryProperty') {
							// Determine binary property name
							let binaryPropertyName: string;

							if (inputDataFormat === 'auto') {
								// Auto-detect: find first binary property
								const binaryData = items[i].binary;

								if (
									!binaryData ||
									typeof binaryData !== 'object' ||
									Object.keys(binaryData).length === 0
								) {
									throw new NodeOperationError(
										this.getNode(),
										`No binary data found in input. Item has: ${JSON.stringify(Object.keys(items[i]))}. Make sure the previous node outputs binary data (e.g., enable "Keep Binary Data" in Kafka Trigger)`,
										{ itemIndex: i },
									);
								}

								// Use the first binary property found
								binaryPropertyName = Object.keys(binaryData)[0];
							} else {
								// User specified the property name
								binaryPropertyName = this.getNodeParameter('binaryPropertyName', i) as string;
							}

							// Use standard helper to get binary data
							const binaryData = this.helpers.assertBinaryData(i, binaryPropertyName);

							if (binaryData.id) {
								// Data is stored in filesystem
								avroData = await this.helpers.getBinaryDataBuffer(i, binaryPropertyName);
							} else {
								// Data is in memory
								avroData = Buffer.from(binaryData.data, 'base64');
							}
						} else if (inputDataFormat === 'base64String' || inputDataFormat === 'hexString') {
							// Get the encoded data directly (supports expressions)
							const encodedData = this.getNodeParameter('encodedData', i) as string;

							if (typeof encodedData !== 'string' || !encodedData) {
								throw new NodeOperationError(
									this.getNode(),
									'Encoded data is empty or invalid. Please provide a valid encoded string.',
									{ itemIndex: i },
								);
							}

							// Decode based on format
							if (inputDataFormat === 'base64String') {
								avroData = Buffer.from(encodedData, 'base64');
							} else {
								avroData = Buffer.from(encodedData, 'hex');
							}
						} else {
							throw new NodeOperationError(
								this.getNode(),
								`Unknown input data format: ${inputDataFormat}`,
								{ itemIndex: i },
							);
						}

						// Deserialize the Avro data
						const decodedData = await registry.decode(avroData);

						// Prepare output
						let outputData: INodeExecutionData;

						if (outputPropertyName && outputPropertyName.trim()) {
							// Store in specified property
							outputData = {
								json: {
									...items[i].json,
									[outputPropertyName]: decodedData,
								},
								pairedItem: { item: i },
							};

							// Keep binary data if it exists
							if (items[i].binary) {
								outputData.binary = items[i].binary;
							}
						} else {
							// Replace entire item
							outputData = {
								json: decodedData as IDataObject,
								pairedItem: { item: i },
							};
						}

						returnData.push(outputData);
					} catch (error) {
						const errorMessage = error instanceof Error ? error.message : String(error);
						if (this.continueOnFail()) {
							returnData.push({
								json: {
									error: errorMessage,
								},
								pairedItem: { item: i },
							});
							continue;
						}
						throw new NodeOperationError(
							this.getNode(),
							`Failed to deserialize Avro data for item ${i}: ${errorMessage}`,
							{ itemIndex: i },
						);
					}
				}
			}

			return [returnData];
		} catch (error) {
			if (this.continueOnFail()) {
				const errorMessage = error instanceof Error ? error.message : String(error);
				return [[{ json: { error: errorMessage } }]];
			}
			throw error;
		}
	}
}
