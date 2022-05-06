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
  return Promise.resolve([
    { name: "name", type: ISparkDataType.STRING },
    { name: "age", type: ISparkDataType.INTEGER },
    { name: "height", type: ISparkDataType.INTEGER },
  ]);
}

export async function requestExecution(request: IExecutionRequest) {
  console.log(request);
}
