import React from "react";

import { Classes } from "@blueprintjs/core";

import { HotkeysProvider } from "@blueprintjs/core";
import { Column, Table2, Cell, TableLoadingOption } from "@blueprintjs/table";
import { ISchemaEntry } from "./api";

interface Props {
  loading: boolean;
  schema: ISchemaEntry[];
  data: any[];
}

function CustomTable(props: Props) {
  const transposedData = props.data.length > 0 ? transpose(props.data) : null;
  const renderName = (name: string, index?: number) => (
    <div style={{ lineHeight: "24px" }}>
      <div className={Classes.TEXT_LARGE}>
        <strong>{name}</strong>
      </div>
      {index !== undefined && (
        <div className={Classes.MONOSPACE_TEXT}>
          {props.schema[index]?.type}
        </div>
      )}
    </div>
  );
  return (
    <HotkeysProvider>
      <Table2
        numRows={
          transposedData
            ? transposedData[Object.keys(transposedData)[0]].length
            : 10
        }
        loadingOptions={props.loading ? [TableLoadingOption.CELLS] : undefined}
      >
        {transposedData
          ? Object.keys(transposedData).map((columnName, i) => (
              <Column
                name={columnName}
                nameRenderer={renderName}
                cellRenderer={(j) => (
                  <Cell>{transposedData[columnName][j]}</Cell>
                )}
              />
            ))
          : props.schema.map((column) => (
              <Column
                name={column.name}
                nameRenderer={renderName}
                cellRenderer={(j) => <Cell />}
              />
            ))}
      </Table2>
    </HotkeysProvider>
  );
}

const transpose = (data: any[]) => {
  return Object.fromEntries(
    Object.keys(data[0]).map((key) => [key, data.map((o: any) => o[key])])
  );
};

export default CustomTable;
