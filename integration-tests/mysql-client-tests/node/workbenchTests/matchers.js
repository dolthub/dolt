function matcher(rows, exp, exceptionKeys, getExceptionIsValid) {
  // Row lengths match
  if (rows.length !== exp.length) {
    return false;
  }
  for (let i = 0; i < rows.length; i++) {
    const rowKeys = Object.keys(rows[i]);
    const expKeys = Object.keys(exp[i]);
    // Row key lengths match
    if (rowKeys.length !== expKeys.length) {
      return false;
    }
    // Row key values match
    for (let j = 0; j < rowKeys.length; j++) {
      const rowKey = rowKeys[j];
      // Check if key has an exception function
      if (exceptionKeys.includes(rowKey)) {
        const isValid = getExceptionIsValid(rows[i], rowKey, exp[i]);
        if (!isValid) {
          console.log("exception was not valid for key", rowKey);
          return false;
        }
      } else {
        // Compare cell values
        const cellVal = JSON.stringify(rows[i][rowKey]);
        const expCellVal = JSON.stringify(exp[i][rowKey]);
        if (cellVal !== expCellVal) {
          console.log("values don't match", cellVal, expCellVal);
          return false;
        }
      }
    }
  }
  return true;
}

function commitHashIsValid(commit) {
  return commit === "STAGED" || commit === "WORKING" || commit.length === 32;
}

function dateIsValid(date) {
  return JSON.stringify(date).length > 0;
}

export function branchesMatcher(rows, exp) {
  const exceptionKeys = ["hash", "latest_commit_date"];

  function getExceptionIsValid(row, key) {
    const val = row[key];
    switch (key) {
      case "hash":
        return commitHashIsValid(val);
      case "latest_commit_date":
        return dateIsValid(val);
      default:
        return false;
    }
  }

  return matcher(rows, exp, exceptionKeys, getExceptionIsValid);
}

export function logsMatcher(rows, exp) {
  const exceptionKeys = ["commit_hash", "date", "parents", "commit_order"];

  function getExceptionIsValid(row, key, expRow) {
    const val = row[key];
    switch (key) {
      case "commit_hash":
        return commitHashIsValid(val);
      case "date":
        return dateIsValid(val);
      case "parents":
        return (
          val.split(", ").filter((v) => !!v.length).length ===
          expRow.parents.length
        );
      case "commit_order":
        return typeof val === "number" && val > 0;
      default:
        return false;
    }
  }

  return matcher(rows, exp, exceptionKeys, getExceptionIsValid);
}

export function mergeBaseMatcher(rows, exp) {
  if (rows.length !== 1 || exp.length !== 1) {
    return false;
  }
  Object.keys(exp).forEach((key) => {
    if (rows[key].length !== 32) {
      return false;
    }
  });
  return true;
}

export function mergeMatcher(rows, exp) {
  const exceptionKeys = ["hash"];

  function getExceptionIsValid(row, key) {
    const val = row[key];
    switch (key) {
      case "hash":
        return commitHashIsValid(val);
      default:
        return false;
    }
  }

  return matcher(rows, exp, exceptionKeys, getExceptionIsValid);
}

export function tagsMatcher(rows, exp) {
  const exceptionKeys = ["tag_hash", "date"];

  function getExceptionIsValid(row, key) {
    const val = row[key];
    switch (key) {
      case "tag_hash":
        return commitHashIsValid(val);
      case "date":
        return dateIsValid(val);
      default:
        return false;
    }
  }

  return matcher(rows, exp, exceptionKeys, getExceptionIsValid);
}

export function diffRowsMatcher(rows, exp) {
  const exceptionKeys = ["to_commit_date", "from_commit_date"];

  function getExceptionIsValid(row, key) {
    const val = row[key];
    switch (key) {
      case "to_commit_date":
      case "from_commit_date":
        return dateIsValid(val);
      default:
        return false;
    }
  }

  return matcher(rows, exp, exceptionKeys, getExceptionIsValid);
}

export function patchRowsMatcher(rows, exp) {
  const exceptionKeys = ["to_commit_hash", "from_commit_hash"];

  function getExceptionIsValid(row, key) {
    const val = row[key];
    switch (key) {
      case "to_commit_hash":
      case "from_commit_hash":
        return commitHashIsValid(val);
      default:
        return false;
    }
  }

  return matcher(rows, exp, exceptionKeys, getExceptionIsValid);
}
