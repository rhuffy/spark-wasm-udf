import React from "react";

import { HotkeysProvider } from "@blueprintjs/core";
import { Column, Table2, Cell } from "@blueprintjs/table";

interface Props {
  data: string[][];
}

function CustomTable(props: Props) {
  return (
    <HotkeysProvider>
      <Table2 numRows={props.data[0]?.length ?? 0}>
        {props.data.map((column) => (
          <Column cellRenderer={(i) => <Cell>column[i]</Cell>} />
        ))}
      </Table2>
    </HotkeysProvider>
  );
}

export default CustomTable;
