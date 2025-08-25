function randint(n)
{
    return int(n * rand())
}
function valueslines(n)
{
    end = "),"
    for (i = 0; i < n; i++) {
        if (i == n-1) {
            end = ");"
        }
        print "(" randint(65536) ",", randint(65536) ",", randint(65536) ",", randint(65536) end;
    }
}
BEGIN {
    print "DROP TABLE IF EXISTS vals;";
    print "CREATE TABLE vals (c1 int, c2 int, c3 int, c4 int);";
    for (j = 0; j < 256; j++) {
        print "INSERT INTO vals VALUES";
        valueslines(1024);
    }
}

