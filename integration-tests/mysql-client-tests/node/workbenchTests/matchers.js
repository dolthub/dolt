export function branchesMatcher(rows, exp) {
  if (rows.length !== exp.length) {
    return false;
  }
  for (let i = 0; i < rows.length; i++) {
    if (rows[i].name !== exp[i].name) {
      return false;
    }
    if (rows[i].hash.length !== 32) {
      return false;
    }
    if (rows[i].latest_commit_date.length === 0) {
      return false;
    }
    if (rows[i].latest_committer !== exp[i].latest_committer) {
      return false;
    }
    if (rows[i].latest_committer_email !== exp[i].latest_committer_email) {
      return false;
    }
  }
  return true;
}

export function logsMatcher(rows, exp) {
  if (rows.length !== exp.length) {
    return false;
  }
  for (let i = 0; i < rows.length; i++) {
    if (rows[i].message !== exp[i].message) {
      return false;
    }
    if (rows[i].commit_hash.length !== 32) {
      return false;
    }
    if (rows[i].date.length === 0) {
      return false;
    }
    if (rows[i].committer !== exp[i].committer) {
      return false;
    }
    if (rows[i].email !== exp[i].email) {
      return false;
    }
    if (exp[i].parentsLength !== undefined) {
      if (
        rows[i].parents.split(", ").filter((v) => !!v.length).length !==
        exp[i].parentsLength
      ) {
        return false;
      }
    }
  }
  return true;
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
