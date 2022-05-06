import axios from "axios";

const URI = "https://localhost:8000";

export interface IFile {
  id: string;
  name: string;
}

export interface ISchemaEntry {
  name: string;
  type: ISparkDataType;
}

export interface IExecutionRequest {
  program: string;
  operation: IOperation;
  functionName: string;
  inputColumnNames: string[];
  outputColumnName?: string;
}

export interface IExecutionResponse {
  resultPath: string;
}

export enum IOperation {
  MAP = "MAP",
  FILTER = "FILTER",
}

export enum ISparkDataType {
  INTEGER = "INTEGER",
  FLOAT = "FLOAT",
  STRING = "STRING",
  BOOLEAN = "BOOLEAN",
}

export function getFiles(): Promise<IFile[]> {
  return Promise.resolve([
    { id: "1", name: "data1.csv" },
    { id: "2", name: "data2.csv" },
  ]);
}

export function getSchemaForFile(file: IFile): Promise<ISchemaEntry[]> {
  return await axios.get(`${URI}/columns`);
}

export async function requestExecution(
  request: IExecutionRequest
): Promise<IExecutionResponse> {
  console.log("ExecutionRequest", request);
  return (await axios.post(`${URI}/execute`, request)).data;
}
