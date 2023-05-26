<?php
$user = $argv[1];
$port = $argv[2];
$db = $argv[3];

$conn = mysqli_connect('127.0.0.1', $user, '', $db, $port)
    or die('Could not connect: ' . mysql_error());

$queries = [
    "create table test (pk int, `value` int, primary key(pk))" => 0,
    "describe test" => 2,
    "insert into test (pk, `value`) values (0,0)" => 0,
    "select * from test" => 1,
    "call dolt_add('-A');" => 1,
    "call dolt_commit('-m', 'my commit')" => 1,
    "call dolt_checkout('-b', 'mybranch')" => 1,
    "insert into test (pk, `value`) values (1,1)" => 0,
    "call dolt_commit('-a', '-m', 'my commit2')" => 1,
    "call dolt_checkout('main')" => 1,
    "call dolt_merge('mybranch')" => 1,
    "select COUNT(*) FROM dolt_log" => 1
];

foreach ($queries as $query => $expected) {
    $result = mysqli_query($conn, $query);
    if (is_bool($result)) {
        if (!$result) {
            echo "LENGTH: {mysqli_num_rows($result)}\n";
            echo "QUERY: {$query}\n";
            echo "EXPECTED: {$expected}\n";
            echo "RESULT: {$result}";
            exit(1);
        }
    } else if (mysqli_num_rows($result) != $expected) {
        echo "LENGTH: {mysqli_num_rows($result)}\n";
        echo "QUERY: {$query}\n";
        echo "EXPECTED: {$expected}\n";
        echo "RESULT: {$result}";
        exit(1);
    }

}

mysqli_close($conn);

exit(0)
?>
