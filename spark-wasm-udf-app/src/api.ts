import axios from "axios";

const URI = "http://localhost:5000/api";

export interface IFile {
  // id: string;
  name: string;
}

export interface ISchemaEntry {
  name: string;
  type: ISparkDataType;
}

export interface IExecutionRequest {
  program: string;
  data: string;
  operation: IOperation;
  function_name: string;
  input_column_names: string[];
  output_column_name?: string;
  output_column_type?: ISparkDataType;
}

export interface IExecutionResponse {
  id: string;
}

export interface IGetFilesResponse {
  names: string[];
}

export interface IGetSchemaResponse {
  schema: ISchemaEntry[];
}

export interface IResultRequest {
  id: string;
}

export interface IResultResponse {
  data?: any[];
}

export enum IOperation {
  MAP = "MAP",
  FILTER = "FILTER",
}

export enum ISparkDataType {
  INTEGER = "INTEGER",
  FLOAT = "FLOAT",
  // STRING = "STRING",
  BOOLEAN = "BOOLEAN",
}

export async function getFiles(): Promise<IGetFilesResponse> {
  return (await axios.get(`${URI}/files`)).data;
}

export async function getSchemaForFile(
  file: IFile
): Promise<IGetSchemaResponse> {
  return (await axios.get(`${URI}/files/${file.name}/schema`)).data;
}

export async function requestExecution(
  request: IExecutionRequest
): Promise<IExecutionResponse> {
  console.log("ExecutionRequest", request);
  const res = await axios.post(`${URI}/execute`, request);
  console.log("ExecutionResponse", res.data);
  return res.data;
}

export async function requestExecutionResult(
  request: IResultRequest
): Promise<IResultResponse> {
  console.log("poll result", request);
  const res = await axios.get(`${URI}/execute/${request.id}`);
  console.log(res.data);
  return res.data;
}
