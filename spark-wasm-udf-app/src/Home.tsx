import React from "react";

import Editor, { Monaco } from "@monaco-editor/react";

import { Button, ControlGroup, MenuItem } from "@blueprintjs/core";
import { MultiSelect, Select } from "@blueprintjs/select";
import {
  getSchemaForFile,
  getFiles,
  IFile,
  ISchemaEntry,
  requestExecution,
  IOperation,
  ISparkDataType,
} from "./api";
import CustomTable from "./CustomTable";

interface Props {}
interface State {
  files: IFile[];
  columns: ISchemaEntry[];
  selectedFileName: string | null;
  selectedColumns: ISchemaEntry[];
  selectedOperation: IOperation;
  selectedOutputType: ISparkDataType | null;
  userFunctionName: string;
}

class Home extends React.Component<Props, State> {
  editor: any = null;

  constructor(props: Props) {
    super(props);
    this.state = {
      files: [],
      columns: [],
      selectedFileName: null,
      selectedColumns: [],
      selectedOperation: IOperation.MAP,
      selectedOutputType: null,
      userFunctionName: "",
    };
  }

  handleEditorDidMount = (editor: any, monaco: Monaco) => {
    this.editor = editor;
  };

  handleExecute = async () => {
    if (this.editor && this.state.selectedFileName) {
      await requestExecution({
        program: this.editor.getValue(),
        data: this.state.selectedFileName,
        operation: IOperation.MAP,
        function_name: this.state.userFunctionName,
        input_column_names: this.state.selectedColumns.map(
          (column) => column.name
        ),
        output_column_name: `${this.state.userFunctionName}_RESULT`,
        output_column_type: this.state.selectedOutputType ?? undefined,
      });
    }
  };

  loadFileNames = async () => {
    const res = await getFiles();
    this.setState({ files: res.names.map((name) => ({ name })) });
  };
  componentDidMount() {
    this.loadFileNames();
  }
  render() {
    const FileSelect = Select.ofType<IFile>();
    const ColumnMultiSelect = MultiSelect.ofType<ISchemaEntry>();
    const OperationSelect = Select.ofType<IOperation>();
    const TypeSelect = Select.ofType<ISparkDataType>();
    return (
      <>
        <ControlGroup fill>
          <FileSelect
            filterable={false}
            items={this.state.files}
            itemRenderer={(file, { handleClick }) => (
              <MenuItem text={file.name} onClick={handleClick} />
            )}
            onItemSelect={async (item) => {
              const res = await getSchemaForFile(item);
              this.setState({
                columns: res.schema,
                selectedColumns: [],
                selectedFileName: item.name,
              });
            }}
          >
            <Button
              text={this.state.selectedFileName ?? "select file"}
              rightIcon="double-caret-vertical"
            />
          </FileSelect>
          <ColumnMultiSelect
            fill
            items={this.state.columns}
            selectedItems={this.state.selectedColumns}
            itemRenderer={(column, { handleClick }) => (
              <MenuItem
                text={`${column.name}: ${column.type}`}
                onClick={handleClick}
              />
            )}
            onItemSelect={(item) => {
              if (!this.state.selectedColumns.includes(item)) {
                this.setState({
                  selectedColumns: [...this.state.selectedColumns, item],
                });
              }
            }}
            onRemove={(item) =>
              this.setState({
                selectedColumns: this.state.selectedColumns.filter(
                  (column) => column.name !== item.name
                ),
              })
            }
            tagRenderer={(item) => `${item.name}: ${item.type}`}
          />{" "}
        </ControlGroup>

        <ControlGroup>
          <input
            type="text"
            placeholder="Function Name"
            value={this.state.userFunctionName}
            onChange={(event) =>
              this.setState({ userFunctionName: event.target.value })
            }
          />
          <OperationSelect
            filterable={false}
            items={Object.values(IOperation)}
            itemRenderer={(item, { handleClick }) => (
              <MenuItem text={item} onClick={handleClick} />
            )}
            onItemSelect={(selectedOperation) => {
              this.setState({ selectedOperation });
            }}
          >
            <Button
              text={this.state.selectedOperation}
              rightIcon="double-caret-vertical"
            />
          </OperationSelect>
          <TypeSelect
            filterable={false}
            items={Object.values(ISparkDataType)}
            itemRenderer={(item, { handleClick }) => (
              <MenuItem text={item} onClick={handleClick} />
            )}
            onItemSelect={(selectedOutputType) =>
              this.setState({ selectedOutputType })
            }
          >
            <Button
              text={this.state.selectedOutputType ?? "select output type"}
              rightIcon="double-caret-vertical"
            />
          </TypeSelect>
        </ControlGroup>

        <Editor
          height="50vh"
          defaultLanguage="c"
          onMount={this.handleEditorDidMount}
          defaultValue={`int add(int a, int b) {
    return a + b;
}`}
        />
        <Button text={"Execute Function"} onClick={this.handleExecute} />
        <br />
        <CustomTable data={[]} />
      </>
    );
  }
}

export default Home;
