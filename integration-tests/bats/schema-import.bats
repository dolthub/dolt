#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    cat <<DELIM > 1pk5col-ints.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
1,1,2,3,4,5
DELIM

    cat <<DELIM > 1pkjsonmap.csv
pk,j
0,"{""a"":1,""b"":""value""}"
DELIM

    cat <<DELIM > 1pkjsonarray.csv
pk,j
1,"[1,2,3]"
DELIM

    cat <<DELIM > 1pklongtext.csv
pk,t
1,imzinjwrqbntxwvroeilncaqtfhmvyapmkfwydcprxuobofasoqqsjikrpbfbiugifrxbudrdxvpngeucqfhkjubuctibzgunzitfpiezccgdosjspbajeoykgvubrnmpxggvdrmjgqebhkzjiihmktiieuxwuxandzgazahztuuslydjxyhtmntslbvfbiknhhaomdeeyljrccskfewldpsunsqtiuxnebzelvlolnxubroofbdacyjizzpscioymhbtmmyfxtcpkccdwgkiyhdbrgnwgaudrqhpvyidvgxelvdmbbetvusfdqsvpvttzxuvgnfoycytavufzpkixewauaaqsofyovcbeyhjwznwzyfrvaqxyrovcywauydgvecnbgedcvqolhtodmrgkskmmavfeotujjbadilledpwkykrjhrpppqamqxbeajjtzrvwpdzowbqksbevexidoltpulwgmlxlrcfxvobjgjscvhpzkatmzbusxhoxidjjudwoultrtaiyilwgwsxurpbcclnnegtmdopzewidzexgbraxywryimsukugxoxyzhghsbqfasmgevrrpntijskjtxvrchuoxbllqijgtvodzdxdobudibkaoghsyfuehftilybmfujrkkxvwnfyonapzyipxlnktcfxmhirfspqkyltdmmwscvyamjwiuagvrqcnrtsibidngypeqcbdqipdlswztmqrwfmsilihaxokcbfhahdwluequvdgiurbomlahecltccevdimtzugehsnuycjlnebnladjvvuvoleamjhlpuspfmgowdahqxizfbsayfhiwfbbyfgljhqzilyqvlpwxytlqdynbyjchgbcaidrbvxldnzmgmsleixgttsclzrutmjoqfqocaifssztaoqsshhlghwoanutrnaylpnijgmkhktvzqteflvnconisnbdssleeuakcqcigssjobqibwzruqqitjmujwhuwefmnozsyftdimilpwczbttkspjitnxmafrdjlabaklsjudbkdqzidtlvjakyandguqcrhqogsxhbfmfvpnyotbswjcsqtxtcvvrsgfmyrwyhqkadoggzmzugwbzazmuigrntjsdyxfmcpapxlqyllbegwscwfpluehtxebgdluffqkgzlbmrjopnefnmlahmubrtmcphakkmgaikjeigfoxszvunwabicsrxiaaodeizqdwdrgczoquvtjuykdvzghrpghvtvvnbnyhswwzfghjduprszznbqsmcbmjkoywbaxbpbnejmzfqnrhilutxnhctsnvmebqyljnpnghvmaziyhhxxonojrjxrvjapqxhmimxpkpkvrmpsoldqjxwpccuayuwvasqgmphpvmtnyojgihlniffdoobdcrgmxnycithvwekvgeyynlckxndmtjoiluecvfivvegyhaiwrqenzaeovugcligvdcrcfwhiiqojajeihxztnhxhqaktnwblvvwrgcvyfbqncgktxsvrrfdesdsxjaoiseoqmcsceospawxabjaladbrjhsninefqybahoezxwdcsqjnmvmzugepiohamlwdgxxjmljnqxzffxjixxhyehsfifoucgkdlnnduollhzwweynujukmnaxnjtynpjwssdhzmdhpkxxxdpymtimozatzfopatlknlluujitqgallwqswlfdgovdymgelractxmckameulrvnurhmpwojnkztbjuwmcrxwzsqenhxwjuwfldqtltagwsnufhsetnromyvfuajivuthqxpplzdzevjahlphnidjowjydclwaeoylvoibtvnnohkepmvdhilspxunubrmxmgqponogwujbtvmcucclvfterepzppbdonqcqhlsjthsiajxmjfkzvkxezroieyqqbbsicuuniqnnigqmzncuctptatlosunxxxfikvgvnigkohxdsopacubidnzcxyxwyexsulzuawzuxecogwpawimrudwgsjfsrjdjgimevisqgvqdotqtzvxogmuvevfmgihsrvcgiuaafoockbqmepmcsjsijyzrksqmcdtuyzcopvsfjyqxmdvnrlrownchedabcrfmrjzbvctbcqfgrxgqtijranslelbmwrrpqnwbookahuavrncyclbcieuoedprgounqgtxygjqkwjqzofiacpvkjsyrjpumxypgixkpldtfpldnmniiiyxbnhodthjlqheyicnpxtlutdmtrxsqgadhxqrwsgimjcmjbpfklwaqsgqxhjqkqgtpnjkwfhwrbnpkjqtefjddqptcshkahkovktjmxibuyzxgjswbdantucqxyqdanprxouopmbanbgwzkbqyohnlbevwxhcuvwvsofhjbgadbgkqusxtnkeaxiyihfkjidtastpplehadkvftpdlivflodekmpdfwsyvbcdttmgicvcqoaehvlgtgbdslwuueuqausjjxufijvonjlfkffmhgaoodmavqpcssqzbvqxdpfbauagvgoutikngdmfyoirihginzwrmotgdyzojahtamsjraxjfyyupnwjhfjgrgyzdlrkqnktxppldjcuaulvimnbccwjcjbynbizpfdfoeblsodjzketiefebxafsfkmsrjyimdjsbkyfyfqimpqdzkmsyfsswhmjefwbtqcteeshebwwzfisaxzexfiooecgoxjfqykshoqtriaiwsnsmfcxtsrmrwrudfmojiyeqglhlbutjvmokejtduojtznosdajgfgnvahhewdvdqflpeasltsqgnbxqnqccnczqrfcrvusbipyblmfupgbbnoqxtgowajozffcsmienpxmlnynsyvealrubnihepvgxbsetyobrfqoabtjhigrlgvbmdaswquahwcddkzsnqovrbikdnylpiemqpsaipnnhmdwitzdqxouedqmomidiuaxhdvhogzjmzezbkikplwnbymqezsndvjfsnervwpwsykwmmjsrazxuxeyctyclxhezhmhhnsmbesrhdnxspdbgcbekyzhlwfaqzwukhtryrpaearbhcjkkgticegjvpoujkouqecwroznscpsrlabfnbolynfptktfjxzwrqkquchszwaqhtgzpcghmcwwhbwbojlphhyjalgrtctqfligyxxajdpcuaopqymczfammytovljaetumbqcyfsukrxdjtifhsdzmqanfjjugyhuqftzhuiqahrltsowvecbkbsxlqswrikbzifjjfgztpbampvebnoupmfbihcraofsqfngxbrsjqpymilbcrquwnbyiduqdwocabptgfqzjnienzhqaltshdhljjvkoxnyslgridqjwgebpzpanwqmfwsczodqmyalfwgmuxrvagkjryvbvqhnzgfgmlpxuejfgmbmjrwlefozibqdgaujxgrskgevwmgeuohciwuwxrighkrlgwecmablejkgxdzxhzexlcxnnexmilmuatknvhaxtnahjfzudrfuytdbounertogvicfncnbbxzglczeevvlywjzbbwuzhqfxshtzkyknnplublkmljkrmanoqhdtqpmfkernutmgxeasvbfhepfokbqwrtysrxemriveuwqtndfbxhihtdyqbmhqgkwibrtxlcvmtajdbqfrfvdbmxkyaeijycdzvvohxtrwbhibtbpwufdsgymjbgeiykclcxemeieuofwacxweoeoaonvawgjvqkzzsanlugdjldcaffqocywheipjpgcsjpnrefuahgbhcucbdtwsalwugjlehiuitdgmsmoqvwtqwzjfkjjkkdpucyowqmfreoebuznwlghnfzgyqtjkpyhrsyunvdhrtkubdbqltyrqdwgoajeqgiaivthzmvwiehgjgfnaqnzthisiwlnnekwhbonjxhbozarowlqsxnzbrmrvdgarorhoeclqcisyqbkfqsxztrekoeayuoavedjaepaapcohdleijxksjbqyejsolwodjyqaymdjerwuglakawdzfhwtptcdhhontmvwgczgcgrtbqzclakgclsnuaxwqcdmgpnedrrecivaaznxyncchazvgtnlrcyvxqbwnrfqrccwapjgpqgxzrezhxxfxfokojirmmhgkcbjchiojwhbqayqkvcxhusxsknjjpenchcbsxgbxshsmwfdyuywcmatrwaamnrnopkqoryldzjkjvpvbdqutdlsjyfsvzgglbbfrrjvgrxtzuedkxymjcfejhdljmdqtimgrxdazicvdiqoyfxzojnrswrxvjktalmvtcpomdiqyyheoiablzmqgiayhksmnqkdpuadmtziplmqihprihrksayriqmpawsupaffmhokqjzxzpfncryohbwooddpnebeivgpklnmyitmagigqrahwmprhgmfghajhlfuqvwyyhrrolyxzhtsookexramcxcktcgxsnswqanfnsvchcozvdngkekstmskpcgetyzmuyaikmqlnoijuohgmegpeatjngirqtwkslkbsmqpwdaalbtiqfoljfauwspigblfwrtvlwlrvkyhlglxnuqkggkjeuwerzmsiajfxuasunfglcrcycbqwqtnllvhqehskqxtnedfrgpwlezwdghcznwlsddheifqrbloiqsxuvqrlxyvwraayzgcdewbopfomhrxiubkovpmdzjsrfhdafltcafxpcxbbyygqmvjijprhlsuswqrxpeudlmnvauzisylomgokpvrybjsxrubjwqhcpptrlfxjvtuchrnohdrfuqahkqqhgydiydsqhzmlbenwbgddtuetlftagfejzesatdgkjxikcdfggcgiabjpzorftvzfnbizpegtaykcioeymllgewxyiomdbevhaxxdwaxhnghdwvftdvqonzjnjpiwfikwowhzmettgpntbrbrfakqhbcmwvutosnugdohagliejtftcdxdgdpyrzsqmqcvpmhvcfpszhmvqyhrgaodiaiijrtseubbubozvnppwbvhrxllurcycwxudnqbegtcbiwovglitxhiwjhydhwsexafkvpzjdgjjrhuwynbdedcbvjxypjspipcnhllrrbokdtzsditkpxkyktnebszbldnsfwlrsyyiylxwoxysqfujuhgvbrfqfxxxgknpbpdftwfhlkysyyedrioopuoisrrrvwhxisiqanlogedfzkfhcamrqugctkgrjeumzgfosjgbbxgaebachrtwrubwsywzqlchpbxediaxsvuevvzxqtticpguladcypsnuenepufumcismswmlxhqkyuwqjjwdoqtdnkeshpuuaaekwayfiwhwmcydgfnmybhwtloidjqunvyezeycqnldljigadwgpoekzqswmetnksmowunatpwwzuudajpmeoqifyolbzsysqndbpbjskhamduqlccgpzwoolxqtymaypnlvlxmzghvunrgtklcjiinrrblefcrmiubvrhkedcmzfqjbpyyrhajctbwcwzrvgrllqhcgccltxajniaxcsylnnwfmfxhfghwlqtzntkivtgzomzjsslkqfktilbufkmsukttrpbevtpuxwvmyhxocgwzafekaahsecewmqhbudxwaadipfephxzctxaxvlruqzyubnptxexhrbkwjyeexdqbnejvexpntdthzcqftttrnkdsysolobwndpcmvnyeqzhmtstfgpzgrmidlmwjrjxeutbepnqtefimygqrjujqjpaumfosikdpxtptjptnvsdzomfgovpqvggnawxdarrtnahjydbouyrkwjruzgpzfgqoiqtdibdwggeugeepblhipobtktufskiwixvdjkhjgnvxsaxwcbjgqehlnurjwnjctwmhafkctwlrqjpvatigdfvuftwvqohoabtyztxhfsreroezhdwapqpsrnkgeszzompdaaxxhmwiqjelkjczmnovotipnrrpgfavexmhqlizcznrjsixgrsmbylahplomeblrfgoxbjsmhhrjskoivgcwdhyylisjdsrucwiezshvjqlstycspegvzlocowdtzmacgplanudotdeqicmvtgzfxtcppkwjfmyjurshhxeggchdegdcsmlasypeioszxfbqvrdhibwubhcvjuzcevushtvolgligzppqsaoymtwthejzajspqixfhgvbbcylumqlfqmfvpnroaogrundewtvbcuiuuzmiqnruowgshjngrdidgkutxqpdzcdnoyujbgqioenuzalpqsquroaiuxduizwhuznlzcmxwpbdgyjejpvluhdxlhmovgogfmaiavjlhgnmiihnjejgeznxianhmtwnorxussxvvwocaiadopwcxupkfrhxeicozmgojnhxnvfttuivoljlsgkrfuhiqyygrrqlnelqrypbvjhmbmzgrhbugdlrsyawozgwahujnwhuwanutoinbgqssdoapfsmvlgvlhnrdmajgwlymojmgrbkictcpllmmlaolpfemkgrcythkazozmgjmjsxehudlkxmgsevszenrbucayevgkwjznnjlssimtvjevwbahcucfantaerorvsimleoblzvztgrbuwrnoufjyoigyujnhexogiqaygcprxzihbeturjlcyawcgwzwfsiegmwgzupdjgayukvdonisgdwwlcsmtqijjkhwevimszxuqslesqmhkylgwhfbbbthsdgvjgbmicrzlveppdqjwqfmwwjxfxuzvpdiflfrfljypqpyvhxkyiwozczooherrwxmupmafubkoyaayxburlcmygmspqwradslurmqbqahklkaesbrbcrvliaunsatytozbjbgadflfebkrtjadjyqftljiksjcmlrjwhhwkywxujtofkqxhlggciupioqwvwvebuknonqqauhbrvybfozmlrudoqhdekbjjclrxgzxntourtjjdnzttqrslunjznqezormkjxjdwcwsvvsupvjvyudakufmqzqqnvnpwmhuqlohzjtsolyupmropvbqqklxfizshjgzgdnjsxykbewdtjzpyytqssgjcgdwuxyikfephhnvnwlpjvbexvdysgqfbjosdfifajzqpukpzpzzwncpfvgnquvokxonaglizskzjdmexrdmseyfjpczqxewgyobxhmywjkufjajlmeqzjembjddxugvuzhioqeqvqkwoypnskadkvraardpzuhlayibxrpmilppbgmrzlzyffaxzzsgukeubzfgequjbtpdvsirjqbtevzcdqlcbsfugrhsvkprmuypagmokhfbvovhzebmgjrdaatonhuaqtsvgghenzxcpspygoyryjytyksbctpoqdgegzphkexyvqmuzvnjuysfgaljnmcvnvjvpodxoqwxzyqpplzdgelzzopcyyrmzshxdpofvzwdrzmjohbutglvkdxmrhdvbnnrzaimwngqsftlcgpxewglpmyivpevhzormedutmwjudbpwzpducqldugeoyuyicmktbevuzmijzdjawusegmevufcfqewweogindtisosnfixmlywoxqjhtbmgqatmokubuqkbbrjciofnrdyekiutsqqitoovmrnwpyigwmhqhchgcvqbxumigexpmszamktgrzwffmllxwhhghqnueryidqktisjhvzpudqykkwgagkgzxxbkcppujbxolhiyqkcfvublusyrdpgvjkqdgrznfjmhuguclyddjccrvaeytvisodqswglazpwwzjlerzxuwqqmikaigwuufudaxilnraeueigwfqcvtxerwnpwxfejnqeiskpetgfllkvwgtcjmgertnweotxthadkwogqrsviluhyfwifiznzzukvbmftxrsfolffzuoxyclmacakyqcvyziuwijjrgboxlmelmplrmcxmvskomztmmpnfalzhsoprqfewypjumhitadnlvrcaqukohhbfetrzctmctkcxbqcskycnvfwzoygvvpopuhfvwpiligtdcdfjgsaipnduorlvopvsirenqsswllgqyomrisleeivemgwggerkswqrdfzcanqidirwrhuunbwfgjnuuceyizfazezvvmahapikuyjxsovvhctvlvqghjmkscmwljteqhtrbzngeyxrpgqfpvuxgqqbordilmhzxsryofeygwozwcszsycfyrbzlokhsznuxtkaoofdqtrukdxhblchlvzimqhbffnihaxeumqjatxwwtwnvkvfvppirczzkdiufgdmzeewfdworpqbdfpntwntdnltzowkvtsrugahcjahsnrnljnmvalypdkalbguzlphlkmccqgddappjjoiclzjpujtbfdwxfnvxqueoscnhrcgfifnvvbfshjdxafwgqvsffvkioncczaguglhmegndesrcxaagsukggzulezqsmmjopujnsvkavgxdwsibamlicckadetbadpjoxxnuohtgrwojyrltiyqbyfqwfncudrqahvkjqvrvsvuqdmqljqogxmoqgebtqimypyqokidtscekkbrorlqsaxfjynotybwbjthgvzaukjvphmrwezmycbxwnlttqxlavgirszjsrchkdeixbyxsnfyrbgzieeystnnawawzmmszbgmkagltcmwuzbarqcxlmbsgxbjbinbyeyhyuwymuxkbpqswkuwdkfjsannfcxdbephnootlxvkgdqccbmlcdnwbojbpjxbntxarzklvkqkkrvzqjhxgxynysrtennksoqiwfalpysocrowcnomqmbwjnpdvikbatrtnixflqtqmytuopxthrogcctqdrcyeccyyzgwuvojtcwvsswprvoaogjqcevwjjshjyilkptvjhruiiazbhcjqatwjybinqsjlppwwovdvgnnxhrrigcinojytaomnxrlpngemxezbgubkimukbtqjpiyxshvfvqnusclvuafmwvgicxhxfhmuzinabvswptvvctotkgjrpsvgouthvklufucjacumyaoscpvwlegtbxcryrgeafurvjnoiqmspboiteclrllukpcjddudorpplxvevwivbmqukpgvomnhxpuqpytwtlokxzqayphmsnztqqkmprtiyersstucbxbosushkazcgwinahamztalkcafaitlksgyipeelvqziqantawlebaeccjjxsavorzpuwxwlygttgzsykymgctwgyqfbisfaajpaihxijprvvgbmafgwmtbczdfasxohtewylijerhsmfcnjzlhuptatkfbfjexblcursmkmhhuhnppppctqxcqxveufzfahbzfkvcwgvhyuomovudbhvnwhdqqmzzyqztkawosauudcujpfdimoknblcgbzcystzfwvyalxsvbtpmhhbrybjnwnknzmkadcsbmhbvohgnndcjfdtvegwhvidyppxacenkpsnqtybpjfcyfmbegjqhnklbkkoaistysojrnquyisxrepdwxtvlxpjhfjzekevkkypexxpinahegmpzmopvhixrmewqszyakmsyzgsuhqexclymlossyvwvtpgwcjztvedccqtcryidmqupwkrdjgiizereajmgixlurbrgrahielwtlehoisceajmfwteslckawgfviogialbtxwrzrxuvkeblyfamikduylmnypowbxehnwesejcesejssbmuuodjyakyshevghcbbpyggwirickqcwodbkfgldlfsrqaqnfhrtmsxmomctmxpzsxptrcxfpjjpmkkylfpkqxskcpznvegiujayyaeuknwkopfunauxmgwtdlkrhbfvusfrxvgjuzcbeoeznilcooccsnyquebtuonekjkhsembqrtigryglqulzrxmpewvmcxurmcytrhxdcepmkoxityliljekkoficoflvedhngajwowdruetpvfezvkfxtmerlijoxgiuekeozqunyouzpafrqjxwmkhczgdwkxwqlzeqehdapgogehvjzeigfnffedzwfgdmbnuhmfalxxfmmwsldzjdkfynnbhspfgiinffdscdkjxzoutwrbptupyydjjsnnqbbmmyuyxwqgahypsilwqlcwdcoyyxkrupnofljiuxwdrnaeeabfrpruebvxzjppcjkyolngzfmmoyrjlcmxkzmbpaffvtegvjwsvozlowaexddkwrarpowmqsjgogkvnebjxrbyjpavmlauhittozprdyfypcytgoscdsqtjmigszonevrzkbnesextihskpdopoqesiyofxhxavmcvynfbcpnqhaniqdieeoejrwkjfdslegesljibbcwwrydouxuwkkikapgmwkjduelbvujojxzftawwamjdbdyltqtayumvkuzxnulksbcttmosxzjkeetrsmgdtpdhaemwdehdhhlmlctvmxaqnpxplkqujsofsiuowlxyptmgtjwevhdwnwczwbcjkhwisgbhnghbhhozxixatkaqmeqtlajajclryreuiugdxuethaedravgjifurfcmoovbcuzdfnzgbvadqdygloktddbenktalpvuyzglcajlblvaftuulqswmkdwsmpiqtytqygwboogjthlefgjmwmyikygwpdddqmrffvxmtiecksgypxtjqbilizculyffgicwtqdzjfeligeocyxjrlboqretfkssirnldfmovxeweplfgeblraelqvuexrurhtwdsizkmxpnntppuujxpeuewaylmwirbqfupwqcxoifadsmcwfoyhlxcpphtckbjfytlunsgbmaoyynwttubehlehhqnpxqdpbunntxpxhsmkhvztlbajnmglwkkcmcxpoqnmkxxderezsblkocpfqoblhqbmzivazjzdqltvvbomzpfxkohkzyovjgdjcntcnuayzbwvbbalboucshezuflknytkajcefuibstolzaeymjtsjrbrwpoopbsztuzbmglyqmaphnklmakuovatkybqfubiirevflkiisksfulpbnctjqevmirdiyyuvpwswlqhoomvckjqkrqtzgushutgnvaqpnnnxtgtjrvrhfceneimksbbrktslgllfzmqsohuckcjgqosyafnqmhggziywomogfrfuphblummkoturmibrpuskhabpxlqfbqwcqyshhrbpraqzcfkieycdbttvhlvxyvatlqctqzplpmwwrtxxtnxmvoehqejvgsoauecwidnleeqprjqzsigsqbxerludnagjaawflqftrikulrgspchkikuhwlsnksihpllqcqcbctxvlzmmgvxkgvozdqfcfuupwuhagcqlcslzfuabtpcjpszjeakbdjhdwqjvnqklwddwqfyfqurggmgsiwbppruiullyatfhdqslcvzgmcibyskfqcplloaoejijlhyxebfywnchkkfewyyxjwihmiasxoljvgmkuolmcvqedbesmidaoizmjujpotdwuymoatxlbhtixzibyqzjzosdtscvkrpitzjcohttmjbicruafgofqjvtcwsbnminynpbibhblbxpnnqoacsdysakodblvcgbxxislkrutidjwpztmsluzabvknsgtytixelizxjlxgnlbgeicigofaaymodujdzbvspfhywjkgtjlhewnayrztkmdeoandefxhayguselqdvpngucrucwgecqpgwerhaejpxwvdudhkrynyjzqadnuymrismhfwftrmvqfciztsyhfmptlzuhjdoabwvjgkjpbhirelxnefyodtrqqtidtjkymlvhseimfenxjjfpitiolkcbgeehiszsbjrwpdfyevhghhytbirtueqkijoujixfioayyoejpunrsbcftcqlgloobpynnasevgoomtiuzgdpymvhqogsdoebjnxdaarziajqyonfqysmsgifsoiskjuzhxgzgoyfdmqcgvhzuopezwokztzroruingkwbhluhtzcbqnvbfoturwswnvvcczffypxsbvtepykicpcbjhmpwkvvinkuhiqdxcfbqvrirbizwxdsssccynvsvpvsyslqpwbawdblgtzwmcootoolnrphgqnnsnxvvyobratcucejfqnzmesvbfjipewwlsurmmqsfiydxycmcsuaeuifswrtxijjntrhosuunxolsupcgwlowgvpilllayzylocqapktnazqdjrxlctrlwnjplrpgsqjosmqxuohrjrqakhuivmwjtwnytkisvmxoipvglkchlulzmwjpzqhfqocyvudahxtsyuwzzppbrprazyxdibmxnnmbrmdzganroswistlyapvkgkzswkgjwsfhayrehpjgzwdxfdujtlumrtsgcxlaygyesdcepxgsqwswqrujhgbvgcvomvretwfgqrknhrrklurtqudezlvuojewbvufysigpqfwqwcbsuepubacmlpjbkjkzonjyagbdqtzeuujjtmnpyzjwvsoetkhfvbwbjcjifadckpxsarsxavkmyzcgzenmujtnolagrxgitzfrzzmlsysngbptlplkvbxklvicvgoxopohnpunnnhbdfocyqysihwawvlpcttbrwnbcnacjwepwlkeqdrpecyjwrdpwbidrltrwuaqqrzwtxgvpiwbptldffmlejaowzhpwzrzqmwqcyifpknmhrlfcfcmatksnzgspbhcwtomojgdroobptadwhtcxbalsvqflianuigbqaotnlapdpujbnplwkmcbekfncwfkjtwxyixgrptdvsqhrirpdemdaiebzgoucidvzqxnipbgqxpveqkvugfzwqervxgtqwprxbtxdpgajtoklxoswwbovwnyjtylwpctyqmaijhxreaclzzkrqgddeljslbopjqdntddwlmlpbroavdqmmupxnxawnwypxgamwoqwdugebsfequutrrtspisnrmtouqiizpzgnfgbfqhitwckzrcfwnixbyjbfmnqjdicbfhlzxmikmrbgqycpweluoqeultyunnmyyeohtkyfbmxpxnhfqjhkhrgrqwfpsdtkscrctrzenopxzyqtkhqljtoylvrzzolqeazhfxhhyhywfthvilxuugydlqmsvmtadwkypelbmbyxytdgtrgfucnjgsmuaeaiubyzqjwbwkwjvcmgkhejfwgkjmjdqrzuzveipfjbswvrctnqdiceeydgsorqrinfveumtdbwmgkyxzvnufyjlaxjcjudsgttzwgluvbajnnjcobkctgwiiedonudfvcgkqkukbzoanxlsxpagsscgeyyqyzasnvtzbrhnoljmuktsyahqgialfdmnifqhfahiaimlrvovvmodkdduyiccserjuqiwhpneuccglckstuwskapbmpfjgfodgvrjadyvzgromfrfinkkecnityecmnjzrigxybjwhhtguwsstgykxeyjesfghnivepcigfykiornhenfttgrehuqiexccfbtyzhnfsdgfrpxpjujctppvttlddxopwydzrbkpqykwbkksgyvmciyelrnzbfltcvoqmjdxomonopnqyevrdyxxvypxsxlevvebppdbcugxcpumripjdsznptnmvmabfngavlfsmqcpcgfgavgdovynkhzpvnuxelcbchtfezcecvwgwwoyydywyrfwtbrchawfylgappclgdqxjhcdjuulvrmggfixszqwdjfpuuxruftgdjovwwnyvnheywjxbybzgdlqbhdnykckuizdahmtkkzytuqfhepebrjvdsoykvmseunwizmowtqxffrcqtgcgrvbxietchnerrzjkoxwnouyiclfiltmttbmcgoqrpsfovetwchkenjcpisqgxnnsmgxeidmazesdgztdwdwtskmgzzcqvizirpiflydzoghvmboudmqlwgpdteueuwpteqrxwjpfxlzlfynylyaihksoddtlotmhkrgiuawbmcaafzsgzncivlxcbdoaxfviollbegiddvsmzdofiemmsqzfrwcvlqmdqitkusrnlpqpbfejerhmtxouwonksagjvztehckfwusgauehlgzrmxnpvlmrpfzxqhabhhklwwjveyomaqlozalbecvzcgyqdixgzvlxclxhzwqnhmwpmdwbigrxjysokfuqrhmtcsukmrgplzrotephvgcssborhslgxcyqbppewbxbdchmhlffyqnxajswztvhsgzkfwevnggreqeogkhytuopvfaarbezcmizwjppbfkfnwikezbgxeygiesxpgegvbspeofvwtcipjwnusszmuqxbbirqivtuzpwsypbzalpcjnddrsvrvyzeiydikcntajssvevkmwhcmzbydyyegkhysvolyplbhusgculxzpxqggbpkjghyqlldxgodnleodfejqzhocsupvlbmfrtzfnszukqsjlyzwljtucivlqldzrxakcwxkguosmqtppnpktbfxerpxzooseaaehhzbixoqszhrvmrvuidgxvbjqxlfptsoltrkzivuhdwfaoslifcifkbbcydghljekdinqwsiqdofgdfkmgpwvfukclpreofviiwewewgibwzydgemrcdxzjiimsoqhmrogorrkxuyeyhwofiwhxuabjrdeijdnnnshdiexqeaufhlbyciqrvwrsknoqtdfogdeaegtwawokgpvovwcriwscbsbbgadhlwqvdmkecfyrcbpxezicuvviuimefjdmibnztsrltwygepnozstpfrvkaisvcobdprkgekufythdxgjebzeufdudhrngodskzkxcfuoqwsbpiakcvgucarqgdrgnoxjrqhphucgutuwqhhpxebbqglntdtslwxfvgvtkdwqpweworlksyqxaiiaezkwrngrgsluadvqlqhblnlrdfwquaqnygynfciqyieqgyncfffsuhumqxrhgqdzzddginjidzfwqjnxrcfevscmpxbgitnkksnplwdsrqmrhaclpirhbhqlwcgubbsiyrnhjoeohhhplobegyepjqeiitywqpcqgwnkpmqfdpdpxvedywxghjoqcpjqsgczivzmdomhgdlgllmwfffojljmzpqijcpkmmszvxovrwvphbzrirzappfblblelnhggfayrunxparxkjblctpplhezkcxtxfxbnrejjqtgmtaoocacojvvahluwspxchgyhhfceygbkffdjvosbqticxtdekaddjkvlucmasuixpwumvfmqwdovjmzxqzzzknmsvxtyqepaqbyzzelbvrgtoykigsqjeaioqujuaypwduxdigjphikmlvwatxbyacatwwosrvhqoshgitrqhmvmyruipfqijojcmunwmlcqczyypywoelhsronvpkaklyuulnbrlxzsabaitqyckjgxuhdjuyayqyvxkjpqfsyjjhtafmckivmnvssohwhnrhbyqrumbatvjrivlslwfwjeguwbcviuzqoyywtjokrgfsdhqwipfvkzhunqcrdszrmpzwojechlhzfbsqmwdxdnrknunvnquzvzydxhhxxfxcuzrhqoeenxulqtiuiljmbsimgnxkbjuvesbeopznjmhqswatiudoagrzwmqtwtivtjhwivjkpuwyrxfplyotkmkzgimkvxhpqoqjgwtzpctruylffrardbipmmqnmgdobvhhhnnnfsiacnhrswggefjxjwqiiattumeuwjltpdgnxgbuoygnxcfvgrifxdjbbfuzrstzjckmqnajyjulpsicaavnfenjhwntlfjzvjlxlxbgbbyzatomxtdwoemyjodiukowizngxhkqswcorceflxerlieqndvgudopqdlngpopbwvsqgqhctzeuzrywbhypzhftigusocddftbfajfclpvwcdwsybyomvoucdyyywfvrtcmanojooamdvbxgsbfxnxvvjuuiccbwmxrdfblcmszdgkgziolhyoiktikhxfvdjkwwxyrzxpwdhpnprdpgpnfqxeaqrjjorvdqbcsevzvmuxtyicgxcwgyugdiucyougwahnfxfnl
DELIM

    cat <<DELIM > 1pksupportedtypes.csv
pk, int, string, boolean, float, uint, uuid
0, 0, "asdf", TRUE, 0.0, 0, "00000000-0000-0000-0000-000000000000"
1, -1, "qwerty", FALSE, -1.0, 1, "00000000-0000-0000-0000-000000000001"
2, 1, "", TRUE, 0.0, 0, "123e4567-e89b-12d3-a456-426655440000"
DELIM

    cat <<DELIM > abc.csv
pk,a,b,c
0, red,  1.1, true
1, blue, 2.2, false
DELIM

    cat <<DELIM > abc-xyz.csv
pk,a,b,c,x,y,z
0, red,  1.1, true,  green,  3.14, -1
1, blue, 2.2, false, yellow, 2.71, -2
DELIM
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "schema-import: create" {
    run dolt schema import -c --pks=pk test 1pk5col-ints.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt schema show
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ "\`c2\` int" ]] || false
    [[ "$output" =~ "\`c3\` int" ]] || false
    [[ "$output" =~ "\`c4\` int" ]] || false
    [[ "$output" =~ "\`c5\` int" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema-import: dry run" {
    run dolt schema import --dry-run -c --pks=pk test 1pk5col-ints.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 9 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ "\`c2\` int" ]] || false
    [[ "$output" =~ "\`c3\` int" ]] || false
    [[ "$output" =~ "\`c4\` int" ]] || false
    [[ "$output" =~ "\`c5\` int" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "test" ]] || false
}

@test "schema-import: import json type" {
    run dolt schema import --dry-run -c --pks=pk test 1pkjsonmap.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`j\` json" ]] || false

    run dolt schema import --dry-run -c --pks=pk test 1pkjsonarray.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`j\` json" ]] || false
}

@test "schema-import: import long text" {
    run dolt schema import --dry-run -c --pks=pk test 1pklongtext.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`t\` text" ]] || false
}

@test "schema-import: with a bunch of types" {
    run dolt schema import --dry-run -c --pks=pk test 1pksupportedtypes.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`int\` int" ]] || false
    [[ "$output" =~ "\`string\` varchar(200)" ]] || false
    [[ "$output" =~ "\`boolean\` tinyint" ]] || false
    [[ "$output" =~ "\`float\` float" ]] || false
    [[ "$output" =~ "\`uint\` int" ]] || false
    [[ "$output" =~ "\`uuid\` char(36) CHARACTER SET ascii COLLATE ascii_bin" ]] || false
}

@test "schema-import: with an empty csv" {
    cat <<DELIM > empty.csv
DELIM
    run dolt schema import --dry-run -c --pks=pk test empty.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Header line is empty" ]] || false
}

@test "schema-import: replace" {
    dolt schema import -c --pks=pk test 1pk5col-ints.csv
    run dolt schema import -r --pks=pk test 1pksupportedtypes.csv
    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`int\` int" ]] || false
    [[ "$output" =~ "\`string\` varchar(200)" ]] || false
    [[ "$output" =~ "\`boolean\` tinyint" ]] || false
    [[ "$output" =~ "\`float\` float" ]] || false
    [[ "$output" =~ "\`uint\` int" ]] || false
    [[ "$output" =~ "\`uuid\` char(36) CHARACTER SET ascii COLLATE ascii_bin" ]] || false
}

@test "schema-import: with invalid names" {
    run dolt schema import -c --pks=pk dolt_docs 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt schema import -c --pks=pk dolt_query_catalog 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt schema import -c --pks=pk dolt_reserved 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
}

@test "schema-import: with multiple primary keys" {
    cat <<DELIM > 2pk5col-ints.csv
pk1,pk2,c1,c2,c3,c4,c5
0,0,1,2,3,4,5
1,1,1,2,3,4,5
DELIM
    run dolt schema import -c --pks=pk1,pk2 test 2pk5col-ints.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table successfully." ]] || false
    dolt schema show
    run dolt schema show
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk1\` int" ]] || false
    [[ "$output" =~ "\`pk2\` int" ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ "\`c2\` int" ]] || false
    [[ "$output" =~ "\`c3\` int" ]] || false
    [[ "$output" =~ "\`c4\` int" ]] || false
    [[ "$output" =~ "\`c5\` int" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false
}

@test "schema-import: missing values in CSV rows" {
    cat <<DELIM > empty-strings-null-values.csv
pk,headerOne,headerTwo
a,"""""",1
b,"",2
c,,3
d,row four,""
e,row five,
f,row six,6
g, ,
DELIM
    run dolt schema import -c --pks=pk test empty-strings-null-values.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` varchar(200)" ]] || false
    [[ "$output" =~ "\`headerOne\` varchar(200)" ]] || false
    [[ "$output" =~ "\`headerTwo\` int" ]] || false
}

@test "schema-import: --keep-types" {
    cat <<DELIM > 1pk5col-strings.csv
pk,c1,c2,c3,c4,c5,c6
"0","foo","bar","baz","car","dog","tim"
"1","1","2","3","4","5","6"
DELIM

    run dolt schema import -c --keep-types --pks=pk test 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "parameter keep-types not supported for create operations" ]] || false
    dolt schema import -c --pks=pk test 1pk5col-ints.csv
    run dolt schema import -r --keep-types --pks=pk test 1pk5col-strings.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ "\`c2\` int" ]] || false
    [[ "$output" =~ "\`c3\` int" ]] || false
    [[ "$output" =~ "\`c4\` int" ]] || false
    [[ "$output" =~ "\`c5\` int" ]] || false
    [[ "$output" =~ "\`c6\` varchar(200)" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema-import: with strings in csv" {
    cat <<DELIM > 1pk5col-strings.csv
pk,c1,c2,c3,c4,c5,c6
"0","foo","bar","baz","car","dog","tim"
"1","1","2","3","4","5","6"
DELIM
    dolt schema import -c --pks=pk test 1pk5col-strings.csv
    run dolt schema import -r --keep-types --pks=pk test 1pk5col-strings.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`c1\` varchar(200)" ]] || false
    [[ "$output" =~ "\`c2\` varchar(200)" ]] || false
    [[ "$output" =~ "\`c3\` varchar(200)" ]] || false
    [[ "$output" =~ "\`c4\` varchar(200)" ]] || false
    [[ "$output" =~ "\`c5\` varchar(200)" ]] || false
    [[ "$output" =~ "\`c6\` varchar(200)" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema-import: supports dates and times" {
    cat <<DELIM > 1pk-datetime.csv
pk, test_date
0, 2013-09-24 00:01:35
1, "2011-10-24 13:17:42"
2, 2018-04-13
DELIM
    run dolt schema import -c --pks=pk test 1pk-datetime.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "datetime" ]] || false;
}

@test "schema-import: uses specific date/time types" {
    cat <<DELIM > chrono.csv
pk, c_date, c_time, c_datetime, c_date+time
0, "2018-04-13", "13:17:42",     "2011-10-24 13:17:42.123", "2018-04-13"
1, "2018-04-13", "13:17:42.123", "2011-10-24 13:17:42",     "13:17:42"
DELIM
    run dolt schema import -c --pks=pk test chrono.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`c_date\` date" ]] || false
    [[ "$output" =~ "\`c_time\` time" ]] || false
    [[ "$output" =~ "\`c_datetime\` datetime" ]] || false
    [[ "$output" =~ "\`c_date+time\` datetime" ]] || false
}

@test "schema-import: import of two tables" {
    dolt schema import -c --pks=pk test1 1pksupportedtypes.csv
    dolt schema import -c --pks=pk test2 1pk5col-ints.csv
}

@test "schema-import: --update adds new columns" {
    dolt table import -c -pk=pk test abc.csv
    dolt sql -q 'delete from test'

    dolt add test
    dolt commit -m "added table"
    run dolt schema import -pks=pk -u test abc-xyz.csv
    [ "$status" -eq 0 ]

    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ '+  `x` varchar(200),' ]] || false
    [[ "$output" =~ '+  `y` float,' ]] || false
    [[ "$output" =~ '+  `z` int,' ]] || false
    # assert no columns were deleted/replaced
    [[ ! "$output" = "-    \`" ]] || false

    run dolt sql -r csv -q 'select * from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,a,b,c,x,y,z" ]] || false
}

@test "schema-import: --update blocked on non-empty table" {
    dolt table import -c -pk=pk test abc.csv
    dolt add test
    dolt commit -m "added table"
    
    run dolt schema import -pks=pk -u test abc-xyz.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "will delete all row data" ]] || false
    [[ "$output" =~ "dolt sql -q 'delete from test'" ]] || false

    dolt sql -q 'delete from test'
    dolt schema import -pks=pk -u test abc-xyz.csv
}

@test "schema-import: --replace adds new columns" {
    dolt table import -c -pk=pk test abc.csv
    dolt sql -q 'delete from test'

    dolt add test
    dolt commit -m "added table"
    run dolt schema import -pks=pk -r test abc-xyz.csv
    [ "$status" -eq 0 ]

    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ '+  `x` varchar(200),' ]] || false
    [[ "$output" =~ '+  `y` float,' ]] || false
    [[ "$output" =~ '+  `z` int,' ]] || false
    # assert no columns were deleted/replaced
    [[ ! "$output" = "-    \`" ]] || false

    run dolt sql -r csv -q 'select count(*) from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "count(*)" ]] || false
    [[ "$output" =~ "0" ]] || false
}

@test "schema-import: --replace blocked on non-empty table" {
    dolt table import -c -pk=pk test abc.csv
    dolt add test
    dolt commit -m "added table"

    run dolt schema import -pks=pk -r test abc-xyz.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "will delete all row data" ]] || false
    [[ "$output" =~ "dolt sql -q 'delete from test'" ]] || false

    dolt sql -q 'delete from test'    
    dolt schema import -pks=pk -u test abc-xyz.csv
}

@test "schema-import: --replace drops missing columns" {
    cat <<DELIM > xyz.csv
pk,x,y,z
0,green,3.14,-1
1,yellow,2.71,-2
DELIM
    dolt table import -c -pk=pk test abc-xyz.csv
    dolt add test
    dolt sql -q 'delete from test'

    dolt commit -m "added test"
    run dolt schema import -pks=pk -r test xyz.csv
    [ "$status" -eq 0 ]

    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  `a` varchar(200),' ]] || false
    [[ "$output" =~ '-  `b` float,' ]] || false
    [[ "$output" =~ '-  `c` tinyint(1),' ]] || false
    # assert no columns were added
    [[ ! "$output" = "+    \`" ]] || false
}

@test "schema-import: with name map" {
    cat <<JSON > name-map.json
{
    "a":"aa",
    "b":"bb",
    "c":"cc"
}
JSON
    run dolt schema import -c -pks=pk -m=name-map.json test abc.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`aa\`" ]] || false
    [[ "$output" =~ "\`bb\`" ]] || false
    [[ "$output" =~ "\`cc\`" ]] || false
    [[ ! "$output" =~ "\`a\`" ]] || false
    [[ ! "$output" =~ "\`b\`" ]] || false
    [[ ! "$output" =~ "\`c\`" ]] || false
}

@test "schema-import: failed import, duplicate column name" {
    cat <<CSV > import.csv
abc,Abc,d
1,2,3
4,5,6
CSV
    run dolt schema import -c -pks=abc test import.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "name" ]] || false
    [[ "$output" =~ "invalid schema" ]] || false
}

@test "schema-import: varchar(200) allows many columns" {
    # Test that with varchar(200) default length, we can have 80+ varchar columns
    # This test creates a CSV with many string columns to verify the row size limit isn't hit
    cat <<DELIM > many_varchar_cols.csv
pk,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,c50,c51,c52,c53,c54,c55,c56,c57,c58,c59,c60,c61,c62,c63,c64,c65,c66,c67,c68,c69,c70,c71,c72,c73,c74,c75,c76,c77,c78,c79,c80
1,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,a1,b1,c1,d1,e1,f1,g1,h1,i1,j1,k1,l1,m1,n1,o1,p1,q1,r1,s1,t1,u1,v1,w1,x1,y1,z1,a2,b2,c2,d2,e2,f2,g2,h2,i2,j2,k2,l2,m2,n2,o2,p2,q2,r2,s2,t2,u2,v2,w2,x2,y2,z2,a3,b3,c3,d3
DELIM
    run dolt schema import -c --pks=pk test many_varchar_cols.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table successfully." ]] || false
    run dolt schema show test
    [ "$status" -eq 0 ]
    # Verify that all columns were created with varchar(200)
    [[ "$output" =~ "\`c1\` varchar(200)" ]] || false
    [[ "$output" =~ "\`c80\` varchar(200)" ]] || false
}
