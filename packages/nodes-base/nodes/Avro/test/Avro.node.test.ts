import type { IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import { Avro } from '../Avro.node';

// Mock the SchemaRegistry
jest.mock('@kafkajs/confluent-schema-registry', () => {
	return {
		SchemaRegistry: jest.fn().mockImplementation(() => ({
			decode: jest.fn().mockImplementation(async (buffer: Buffer) => {
				// Mock decode that returns a simple JSON object based on the buffer content
				// For testing purposes, we'll return a predictable result
				if (buffer.length === 0) {
					throw new Error('Empty buffer');
				}

				// Simulate Confluent Schema Registry wire format:
				// First 5 bytes are magic byte + schema ID, rest is Avro payload
				if (buffer.length < 5) {
					throw new Error('Buffer too small for Confluent wire format');
				}

				// Return a mock decoded object
				return {
					id: 'test-id-123',
					timestamp: '1975660258',
					type: 'advertiser',
				};
			}),
		})),
	};
});

describe('Avro Node', () => {
	let avroNode: Avro;
	let mockExecuteFunctions: IExecuteFunctions;

	const mockCredentials = {
		url: 'http://localhost:8081',
		authentication: 'none',
	};

	const mockCredentialsBasicAuth = {
		url: 'http://localhost:8081',
		authentication: 'basicAuth',
		username: 'testuser',
		password: 'testpass',
	};

	beforeEach(() => {
		avroNode = new Avro();

		// Setup base mock functions
		mockExecuteFunctions = {
			getNode: jest.fn().mockReturnValue({
				name: 'Avro',
				type: 'n8n-nodes-base.avro',
			}),
			getInputData: jest.fn().mockReturnValue([]),
			getNodeParameter: jest.fn(),
			getCredentials: jest.fn().mockResolvedValue(mockCredentials),
			helpers: {
				assertBinaryData: jest.fn(),
				getBinaryDataBuffer: jest.fn(),
				prepareBinaryData: jest.fn(),
			},
			continueOnFail: jest.fn().mockReturnValue(false),
		} as unknown as IExecuteFunctions;
	});

	afterEach(() => {
		jest.clearAllMocks();
	});

	describe('Auto-Detect Mode', () => {
		it('should deserialize binary data using auto-detect', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toEqual({
				id: 'test-id-123',
				timestamp: '1975660258',
				type: 'advertiser',
			});
		});

		it('should throw error when no binary data found in auto-detect mode', async () => {
			const inputData: INodeExecutionData[] = [
				{
					json: { message: 'test' },
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);

			await expect(avroNode.execute.call(mockExecuteFunctions)).rejects.toThrow(NodeOperationError);
		});

		it('should handle binary data with id (filesystem reference)', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							id: 'binary-data-id-123',
							data: '',
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				id: 'binary-data-id-123',
				mimeType: 'application/octet-stream',
			});
			(mockExecuteFunctions.helpers.getBinaryDataBuffer as jest.Mock).mockResolvedValue(testBuffer);

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(mockExecuteFunctions.helpers.getBinaryDataBuffer).toHaveBeenCalledWith(0, 'data');
			expect(result[0][0].json).toEqual({
				id: 'test-id-123',
				timestamp: '1975660258',
				type: 'advertiser',
			});
		});
	});

	describe('Binary Property Mode', () => {
		it('should deserialize from specified binary property', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						customProperty: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'binaryProperty';
					if (paramName === 'binaryPropertyName') return 'customProperty';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(mockExecuteFunctions.helpers.assertBinaryData).toHaveBeenCalledWith(
				0,
				'customProperty',
			);
		});
	});

	describe('Base64 String Mode', () => {
		it('should deserialize from base64 encoded string', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const base64String = testBuffer.toString('base64');

			const inputData: INodeExecutionData[] = [
				{
					json: {
						encodedMessage: base64String,
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'base64String';
					if (paramName === 'encodedData') return base64String;
					if (paramName === 'options') return {};
					return undefined;
				},
			);

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toEqual({
				id: 'test-id-123',
				timestamp: '1975660258',
				type: 'advertiser',
			});
		});

		it('should throw error for invalid base64 string', async () => {
			const inputData: INodeExecutionData[] = [
				{
					json: {
						encodedMessage: '',
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'base64String';
					if (paramName === 'encodedData') return '';
					if (paramName === 'options') return {};
					return undefined;
				},
			);

			await expect(avroNode.execute.call(mockExecuteFunctions)).rejects.toThrow(NodeOperationError);
		});
	});

	describe('Hex String Mode', () => {
		it('should deserialize from hex encoded string', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const hexString = testBuffer.toString('hex');

			const inputData: INodeExecutionData[] = [
				{
					json: {
						encodedMessage: hexString,
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'hexString';
					if (paramName === 'encodedData') return hexString;
					if (paramName === 'options') return {};
					return undefined;
				},
			);

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toEqual({
				id: 'test-id-123',
				timestamp: '1975660258',
				type: 'advertiser',
			});
		});
	});

	describe('Output Options', () => {
		it('should store deserialized data in specified output property', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const inputData: INodeExecutionData[] = [
				{
					json: {
						existingField: 'value',
					},
					binary: {
						data: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return { outputPropertyName: 'deserializedData' };
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toEqual({
				existingField: 'value',
				deserializedData: {
					id: 'test-id-123',
					timestamp: '1975660258',
					type: 'advertiser',
				},
			});
		});

		it('should replace entire item when no output property specified', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const inputData: INodeExecutionData[] = [
				{
					json: {
						existingField: 'value',
					},
					binary: {
						data: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {}; // No outputPropertyName
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toEqual({
				id: 'test-id-123',
				timestamp: '1975660258',
				type: 'advertiser',
			});
			expect(result[0][0].json).not.toHaveProperty('existingField');
		});
	});

	describe('Error Handling', () => {
		it('should handle continueOnFail for item-level errors', async () => {
			const testBuffer = Buffer.from(''); // Empty buffer to trigger error
			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});
			(mockExecuteFunctions.continueOnFail as jest.Mock).mockReturnValue(true);

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(result[0][0].json).toHaveProperty('error');
			expect(result[0][0].json.error).toContain('Empty buffer');
		});

		it('should throw error when continueOnFail is false', async () => {
			const testBuffer = Buffer.from(''); // Empty buffer to trigger error
			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});
			(mockExecuteFunctions.continueOnFail as jest.Mock).mockReturnValue(false);

			await expect(avroNode.execute.call(mockExecuteFunctions)).rejects.toThrow(NodeOperationError);
		});
	});

	describe('Credentials', () => {
		it('should initialize SchemaRegistry with no authentication', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});

			await avroNode.execute.call(mockExecuteFunctions);

			expect(mockExecuteFunctions.getCredentials).toHaveBeenCalledWith('schemaRegistryApi');
		});

		it('should initialize SchemaRegistry with basic authentication', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.getCredentials as jest.Mock).mockResolvedValue(
				mockCredentialsBasicAuth,
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});

			await avroNode.execute.call(mockExecuteFunctions);

			expect(mockExecuteFunctions.getCredentials).toHaveBeenCalledWith('schemaRegistryApi');
		});
	});

	describe('Multiple Items', () => {
		it('should process multiple items correctly', async () => {
			const testBuffer1 = Buffer.from('test-avro-data-1');
			const testBuffer2 = Buffer.from('test-avro-data-2');

			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							data: testBuffer1.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
				{
					json: {},
					binary: {
						data: {
							data: testBuffer2.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockImplementation(
				(itemIndex: number) => {
					return {
						data: inputData[itemIndex].binary!.data.data,
						mimeType: 'application/octet-stream',
						id: undefined,
					};
				},
			);

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(2);
			expect(result[0][0].json).toEqual({
				id: 'test-id-123',
				timestamp: '1975660258',
				type: 'advertiser',
			});
			expect(result[0][1].json).toEqual({
				id: 'test-id-123',
				timestamp: '1975660258',
				type: 'advertiser',
			});
		});

		it('should handle errors in some items while continuing with others when continueOnFail is true', async () => {
			const testBuffer1 = Buffer.from(''); // Empty to trigger error
			const testBuffer2 = Buffer.from('test-avro-data-2');

			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							data: testBuffer1.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
				{
					json: {},
					binary: {
						data: {
							data: testBuffer2.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockImplementation(
				(itemIndex: number) => {
					return {
						data: inputData[itemIndex].binary!.data.data,
						mimeType: 'application/octet-stream',
						id: undefined,
					};
				},
			);
			(mockExecuteFunctions.continueOnFail as jest.Mock).mockReturnValue(true);

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(2);
			expect(result[0][0].json).toHaveProperty('error');
			expect(result[0][1].json).toEqual({
				id: 'test-id-123',
				timestamp: '1975660258',
				type: 'advertiser',
			});
		});
	});

	describe('pairedItem', () => {
		it('should include pairedItem in output', async () => {
			const testBuffer = Buffer.from('test-avro-data');
			const inputData: INodeExecutionData[] = [
				{
					json: {},
					binary: {
						data: {
							data: testBuffer.toString('base64'),
							mimeType: 'application/octet-stream',
							fileExtension: 'bin',
							fileName: 'message',
						},
					},
				},
			];

			(mockExecuteFunctions.getInputData as jest.Mock).mockReturnValue(inputData);
			(mockExecuteFunctions.getNodeParameter as jest.Mock).mockImplementation(
				(paramName: string) => {
					if (paramName === 'operation') return 'deserialize';
					if (paramName === 'inputDataFormat') return 'auto';
					if (paramName === 'options') return {};
					return undefined;
				},
			);
			(mockExecuteFunctions.helpers.assertBinaryData as jest.Mock).mockReturnValue({
				data: testBuffer.toString('base64'),
				mimeType: 'application/octet-stream',
				id: undefined,
			});

			const result = await avroNode.execute.call(mockExecuteFunctions);

			expect(result).toHaveLength(1);
			expect(result[0]).toHaveLength(1);
			expect(result[0][0]).toHaveProperty('pairedItem');
			expect(result[0][0].pairedItem).toEqual({ item: 0 });
		});
	});
});
