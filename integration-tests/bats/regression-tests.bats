#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "regression-tests: dolt issue #1081" {
    dolt sql <<"SQL"
CREATE TABLE XA(YW VARCHAR(24) NOT NULL, XB VARCHAR(100), XC VARCHAR(2500),
  XD VARCHAR(2500), XE VARCHAR(100), XF VARCHAR(100), XG VARCHAR(100),
  XI VARCHAR(100), XJ VARCHAR(100), XK VARCHAR(100), XL VARCHAR(100),
  XM VARCHAR(1000), XN TEXT, XO TEXT, PRIMARY KEY (YW));
CREATE TABLE XP(YW VARCHAR(24) NOT NULL, XQ VARCHAR(100) NOT NULL,
  XR VARCHAR(1000), PRIMARY KEY (YW));
CREATE TABLE XS(YW VARCHAR(24) NOT NULL, XT VARCHAR(24) NOT NULL,
  XU VARCHAR(24), XV VARCHAR(100) NOT NULL, XW DOUBLE NOT NULL,
  XX DOUBLE NOT NULL, XY VARCHAR(100), XC VARCHAR(100), XZ VARCHAR(100) NOT NULL,
  YA DOUBLE, YB VARCHAR(24) NOT NULL, YC VARCHAR(1000), XO VARCHAR(1000),
  YD DOUBLE NOT NULL, YE DOUBLE NOT NULL, PRIMARY KEY (YW));
CREATE TABLE YF(YW VARCHAR(24) NOT NULL, XB VARCHAR(100) NOT NULL, YG VARCHAR(100),
  YH VARCHAR(100), XO TEXT, PRIMARY KEY (YW));
CREATE TABLE yp(YW VARCHAR(24) NOT NULL, XJ VARCHAR(100) NOT NULL, XL VARCHAR(100),
  XT VARCHAR(24) NOT NULL, YI INT NOT NULL, XO VARCHAR(1000), PRIMARY KEY (YW),
  FOREIGN KEY (XT) REFERENCES XP (YW));
INSERT INTO XS VALUES ('', '', NULL, 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC', 0, 0,
  NULL, NULL, '', NULL, '', NULL, NULL, 0, 0);
INSERT INTO YF VALUES ('', '', NULL, NULL, NULL);
INSERT INTO XA VALUES ('', '', '', '', '', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC',
  '', '', '', '', '', '', '', '');
SQL
    run dolt sql -r=csv -q "
SELECT DISTINCT YM.YW AS YW, (SELECT YW FROM YF WHERE YF.XB = YM.XB) AS YF_YW,
  (SELECT YW FROM yp WHERE yp.XJ = YM.XJ AND (yp.XL = YM.XL OR (yp.XL IS NULL AND
  YM.XL IS NULL)) AND yp.XT = nd.XT) AS YJ, XE AS XE, XI AS YO, XK AS XK, XM AS XM,
  CASE WHEN YM.XO <> 'Z' THEN YM.XO ELSE NULL END AS XO FROM (SELECT YW, XB, XC, XE,
  XF, XI, XJ, XK, CASE WHEN XL = 'Z' OR XL = 'Z' THEN NULL ELSE XL END AS XL, XM,
  XO FROM XA) YM INNER JOIN XS nd ON nd.XV = XF WHERE XB IN (SELECT XB FROM YF) AND
  (XF IS NOT NULL AND XF <> 'Z') UNION SELECT DISTINCT YL.YW AS YW, (SELECT YW FROM
  YF WHERE YF.XB = YL.XB) AS YF_YW, (SELECT YW FROM yp WHERE yp.XJ = YL.XJ AND
  (yp.XL = YL.XL OR (yp.XL IS NULL AND YL.XL IS NULL)) AND yp.XT = YN.XT) AS YJ,
  XE AS XE, XI AS YO, XK AS XK, XM AS XM, CASE WHEN YL.XO <> 'Z' THEN YL.XO ELSE
  NULL END AS XO FROM (SELECT YW, XB, XC, XE, XF, XI, XJ, XK, CASE WHEN XL = 'Z' OR
  XL = 'Z' THEN NULL ELSE XL END AS XL, XM, XO FROM XA) YL INNER JOIN XS YN ON
  YN.XC = YL.XC WHERE XB IN (SELECT XB FROM YF) AND (XF IS NULL OR XF = 'Z');"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "YW,YF_YW,YJ,XE,YO,XK,XM,XO" ]] || false
    [[ "$output" =~ '"","",,"","","","",""' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "regression-tests: TINYBLOB skipping BlobKind for some values" {
    # caught by fuzzer
    dolt sql <<"SQL"
CREATE TABLE ClgialBovK (
  CIQgW0 TINYBLOB,
  Hg6qI0 DECIMAL(19,12),
  UJ46Q1 VARCHAR(2) COLLATE utf8mb4_0900_ai_ci,
  YEGomx TINYINT,
  PRIMARY KEY (Hg6qI0)
);
REPLACE INTO ClgialBovK VALUES ("WN4*Zx.NI4a|MLLwRc:A9|rsl%3:r_gxLb-YY3c*OaTyuL=-ui!PBRhF0ymVW6!Uey*5DNM9O-Qo=0@#nkK","9993429.437834949734","",-104);
REPLACE INTO ClgialBovK VALUES ("z$=kjmZtGlCbJ:=o9vRCZe70a:1o6tMrV% 2np! CK@NytnPE9BU03iu1@f@Uch=CwB$3|8RLXfnnKh.+H:9oy6X1*IyU_jP|ji4KuG .DOsiO.hk~lBlm5hBxeBQXe-NzNmj=%2c!:V7%asxX!A6Kg@l+Uxd9^9t3a^NUsr3GD5xc=hqyb*QbZk||frmQ+_:","3475975.285903026799","",-9);
SQL
    run dolt sql -q "SELECT * FROM ClgialBovK;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "CIQgW0,Hg6qI0,UJ46Q1,YEGomx" ]] || false
    [[ "$output" =~ 'WN4*Zx.NI4a|MLLwRc:A9|rsl%3:r_gxLb-YY3c*OaTyuL=-ui!PBRhF0ymVW6!Uey*5DNM9O-Qo=0@#nkK,9993429.437834949734,"",-104' ]] || false
    [[ "$output" =~ 'z$=kjmZtGlCbJ:=o9vRCZe70a:1o6tMrV% 2np! CK@NytnPE9BU03iu1@f@Uch=CwB$3|8RLXfnnKh.+H:9oy6X1*IyU_jP|ji4KuG .DOsiO.hk~lBlm5hBxeBQXe-NzNmj=%2c!:V7%asxX!A6Kg@l+Uxd9^9t3a^NUsr3GD5xc=hqyb*QbZk||frmQ+_:,3475975.285903026799,"",-9' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "regression-tests: VARBINARY incorrect length reading" {
    # caught by fuzzer
    dolt sql <<"SQL"
CREATE TABLE TBXjogjbUk (
  pKVZ7F set('rxb9@ud94.t','py1lf7n1t*dfr') NOT NULL,
  OrYQI7 mediumint NOT NULL,
  wEU2wL varbinary(9219) NOT NULL,
  nE3O6H int NOT NULL,
  iIMgVg varchar(11833),
  PRIMARY KEY (pKVZ7F,OrYQI7,nE3O6H)
);
SQL
    dolt sql -q "REPLACE INTO TBXjogjbUk VALUES (1,-5667274,'wRL',-1933632415,'H');"
    dolt sql -q "REPLACE INTO TBXjogjbUk VALUES (1,-5667274,'wR',-1933632415,'H');"
    run dolt sql -q "SELECT * FROM TBXjogjbUk;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pKVZ7F,OrYQI7,wEU2wL,nE3O6H,iIMgVg" ]] || false
    [[ "$output" =~ "rxb9@ud94.t,-5667274,wR,-1933632415,H" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "regression-tests: UNIQUE index violations do not break future INSERTs" {
    skip_nbf_dolt_1
    skiponwindows "Need to install expect and make this script work on windows."
    mkdir doltsql
    cd doltsql
    dolt init

    run $BATS_TEST_DIRNAME/sql-unique-error.expect
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "Error" ]] || false
    [[ ! "$output" =~ "error" ]] || false

    run dolt sql -q "SELECT * FROM test ORDER BY 1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false

    cd ..
    rm -rf doltsql
}

@test "regression-tests: some rows are not found by WHERE filter" {
    # caught by fuzzer
    dolt sql <<"SQL"
CREATE TABLE `grwzHfTFle` (`oyBEOq` varchar(64) COLLATE utf8mb4_bin, `hrbo0L` BLOB, `stSIYd` SMALLINT, `qTiF9V` TINYBLOB, `10a9ox` DOUBLE, `T2p7cl` ENUM('lE','K4drH9','s4y2zIBiGwjL4Rc','J0saTK','wPxfdWEVqYaiGS','QrClJ8DJlSQ','BQ_J2QInKTd','3o8GpiOD','C6OzkhR2GCavFB4','KoW_P86ig8Lb','rmYEkq','7xEAI_zTH8tx','esL8ebM1nALu','yvaI1Q','OmMrF6HVewJC','s_vzApJccj0','95mHsEDfQwP28k','G_CWMFqLM5sUkh5','0vNld','St36B4u','bVCCAq0K2WA_S0','qLjfEKNzOOincWaJ','a9FswYo','OdjFO','yDPuq','BxTQ9Mi_9u4ZA2Q','fpCPGA','uVJnla5','F_RlXpYT03TKdIU6','ytFjALpsQywkTV'), `21sKwG` SMALLINT UNSIGNED, PRIMARY KEY (`oyBEOq`(60), `stSIYd`, `10a9ox`));
INSERT INTO `grwzHfTFle` VALUES ('!JYs-wbJ$34K3g$Y 4 CVaSukaFAt-!onw!%fPZ^a=8','E2mE%FWZ$Dnqj!@1gdztO4R5t:JS9~YCqHO#+*TabR$Wxa*eEi7xwf8nwS572Nm^9!0I.X!%-gQlpzaozZpb~u#bqHF!r7wsY1W=SL7UyYhB8J1%8y:hRD+7~5LzMcIh-WO5HbviWjo0$2Fd2#5hg#==c+=MjuN-ZCt*@285e5N_K4vsps2eY9$uHH3LCrUrHIek8X$#WXtG^a%hpH2VDm PJx1StxK|cF4NI@PWH*1iC!M%w$:!C9-o~FFq6wQW%9xn661!_tm2oWu:b!aS#0G5YW0x-W2Q!XEP@f3CNY4xQ3IoO9|:4Hbc4iw$PnYT_%pL!byJ*=:.8e~p h Syn- SM aFjDm3d-8U#IiN3:jaB1%szEYPoGIsHAZ*Zd56vY7V#BoE%:FZ =-~YJ8_.5AcMDj.x@jm*9HMQ4_u9x6I5$h0PjLp2$+=Bf%Vj_i!*lM es.oHJ@72Ksjb1|:ntzcsm2n+y4EX+A43L|W4!Ukgt43W3!nDf2s-#JqHR%MUycA95tc.uR*xy07iX-03mk',-28878,'V6=Mhy%_PO-!D:8Lg#l6u|8.+=uwERz6F8gf44$dl6THpGp7|4F!#TveAY vR_h7WrWzJKw~f Q1pRXwMN9IfUd6LC:#cXH+@i.v_0P8h%4mVVLcfV!@!ua57','-0.809039007248521',12,25759);
DELETE FROM `grwzHfTFle` WHERE `oyBEOq` = '!JYs-wbJ$34K3g$Y 4 CVaSukaFAt-!onw!%fPZ^a=8' AND `hrbo0L` = 'E2mE%FWZ$Dnqj!@1gdztO4R5t:JS9~YCqHO#+*TabR$Wxa*eEi7xwf8nwS572Nm^9!0I.X!%-gQlpzaozZpb~u#bqHF!r7wsY1W=SL7UyYhB8J1%8y:hRD+7~5LzMcIh-WO5HbviWjo0$2Fd2#5hg#==c+=MjuN-ZCt*@285e5N_K4vsps2eY9$uHH3LCrUrHIek8X$#WXtG^a%hpH2VDm PJx1StxK|cF4NI@PWH*1iC!M%w$:!C9-o~FFq6wQW%9xn661!_tm2oWu:b!aS#0G5YW0x-W2Q!XEP@f3CNY4xQ3IoO9|:4Hbc4iw$PnYT_%pL!byJ*=:.8e~p h Syn- SM aFjDm3d-8U#IiN3:jaB1%szEYPoGIsHAZ*Zd56vY7V#BoE%:FZ =-~YJ8_.5AcMDj.x@jm*9HMQ4_u9x6I5$h0PjLp2$+=Bf%Vj_i!*lM es.oHJ@72Ksjb1|:ntzcsm2n+y4EX+A43L|W4!Ukgt43W3!nDf2s-#JqHR%MUycA95tc.uR*xy07iX-03mk' AND `stSIYd` = -28878 AND `qTiF9V` = 'V6=Mhy%_PO-!D:8Lg#l6u|8.+=uwERz6F8gf44$dl6THpGp7|4F!#TveAY vR_h7WrWzJKw~f Q1pRXwMN9IfUd6LC:#cXH+@i.v_0P8h%4mVVLcfV!@!ua57' AND `10a9ox` = '-0.809039007248521';
SQL
    run dolt sql -q 'SELECT COUNT(*) FROM grwzHfTFle WHERE oyBEOq LIKE "!JYs%"' -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "0" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}
