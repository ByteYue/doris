// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("jira8317") {
    sql """
    DROP TABLE IF EXISTS `table_1000_undef_undef2`;
    """
    sql """
    CREATE TABLE `table_1000_undef_undef2` (
    `pk` int(11) NULL,
    `col_bigint_undef_signed` bigint(20) NULL,
    `col_bigint_undef_signed2` bigint(20) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`pk`, `col_bigint_undef_signed`, `col_bigint_undef_signed2`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`pk`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """
    sql """
        INSERT INTO `table_1000_undef_undef2` VALUES (1,-19003,-120),(7,-7365641,-8081123887699099479),(39,NULL,8157469758106312693),(45,-27,-8123020834936742524),(58,4999,3743594151072369933),(69,655619458377523526,7519664894086114892),(87,8262093739053643222,1766397486317050982),(91,4704,123),(93,-7587996,18142),(102,-417417511588815387,NULL),(105,6230407775549921021,8109),(111,-9885,1417473),(123,-2555577,-5337152),(125,-59,-1),(126,19,-16894),(134,24543,76),(157,5395653578239812058,-1764615),(166,3757370,115714765999722154),(175,-2899615982387187770,6986202),(196,NULL,1447),(199,5068178230655872542,-10674),(205,22,6431560),(218,-15874,NULL),(246,-8735,-5775020777947744263),(265,NULL,13482),(298,9216134360394789591,5679),(308,6845580,-3004454),(322,-6816415254937360604,29758),(341,-4767381996628937761,8171354672951628997),(371,-627487131429034642,251),(374,-16,NULL),(378,-4114549148709048844,-2570924511164827202),(391,29515,NULL),(398,3229973449457755169,9186583667918079554),(403,32,946998133221629324),(406,NULL,NULL),(412,4527414356688238927,-12),(435,-124,5876613814197233863),(438,-30856,NULL),(472,14434,-17744),(477,-5877513137857099418,-3310242399155057576),(492,-1648965159783569768,NULL),(497,5891759987264692585,28119),(504,-3009353251680330192,-3385585),(509,-6742623742434484156,-6746617),(510,2658442546697658903,NULL),(514,-8234,63),(533,-13,2573219),(537,-7946380441594310393,-11702),(543,-91,NULL),(558,6972989139639064837,8925213840422996445),(572,NULL,-4262993),(579,-5897753,-26000),(582,NULL,-1106448149334542023),(585,NULL,-9110552557766495430),(586,19018,-8696199199951370044),(603,-2905465,1065078903442796472),(611,-29913,4000),(613,-25,1334372101623564589),(625,7922,6923419),(631,-5273350,-59),(646,6848743253557587590,NULL),(660,147308,21376),(690,85,2768344),(701,-17302,NULL),(702,NULL,-79),(706,4214846,13065),(715,13968,4374384568269861716),(737,-5211216,-1613482),(743,NULL,NULL),(749,-11,-25175),(764,-5187474377935208440,5528350657610937595),(774,11254,-4067970),(795,-3941881,16531),(798,-7415236,10847),(823,-8442816819368076641,82),(858,99,-6114372),(878,NULL,-2995508),(883,-12837,-5229344807687917760),(889,-31,9331),(903,579841,66183395754462121),(930,-5462323,5620223),(935,-2161998,-5457916537252374932),(941,4455127172594657873,NULL),(959,11161,-27604),(963,-6823471941991501348,247878),(983,28739,813222649038017676),(995,-6880546792162904257,NULL),(12,NULL,-6244283584821915660),(23,-55,NULL),(27,-837602972884855962,32),(32,28665,-2886572565035405541),(50,2735653,-61),(90,5041,-4293116302471282150),(103,5509801503377205293,123),(110,3835650522971313342,6534645771525321721),(121,101,5449358),(136,NULL,-2846700542533112846),(141,-5412553832315383215,4),(142,-1416930957245477234,17),(149,12615,32427),(150,10340,-4956030870704664676),(156,-3280843,7750834006460826023),(159,7577082264394679086,-90),(164,3980,-6133128),(167,-14,-947978666917274289),(168,6871907822690179662,-3859458),(173,-92,14),(176,8137848140098659898,2810201290537783567),(179,-405144302754595661,-7896018842801290229),(201,-786268,23113),(209,-5281,5551649),(229,4144,25571),(257,-7010271,-4838803),(277,959701,3554446),(314,58,44),(343,7743,NULL),(352,2084635494667751228,-10691),(358,842043435432900253,-7193426924333597012),(362,3364256032068110028,-7474997),(375,-339,8057614),(379,NULL,1804970089654852765),(382,3478857192154069111,2127),(389,-624910,88),(390,2940413006456803386,61),(394,0,-6186),(404,4704782582378089789,48568957078751827),(414,-4365630394963277377,6075188749901339503),(431,NULL,-19429),(433,NULL,-2976069229306407708),(434,NULL,1758157016624770386),(439,-83,1319438),(445,-445678981456339247,6764170805906335296),(455,-8805832866437583211,-107),(456,-861505,3001103443915789354),(467,NULL,82),(470,16,-11217),(484,8847033568544232196,-7245108982741225054),(494,-1414,97),(496,29231,-4088969),(505,-429024549165417607,-3329),(506,NULL,2728150847869367661),(524,-4079451130938998056,-1457),(527,-250460,-1713739740231516514),(529,-1168162,NULL),(547,-6833274923109161689,NULL),(553,-3203931376566131833,-6193646836234948352),(561,1325130049773122078,10867),(562,15581,3019322609487292475),(567,NULL,-39),(573,69,NULL),(574,3949615548534239825,-100),(584,NULL,NULL),(587,2261044,7674764774128981957),(598,8204503,880502),(621,-7615727908514562682,NULL),(641,-42,-8123418684499871829),(647,141760341012454652,NULL),(653,-2149175478618130107,3096451179703625471),(673,NULL,6585330350640852279),(686,-3706480083217016813,1943182905989046798),(709,372863230934331886,7699766326928220154),(713,5780453,1497),(714,3828854,NULL),(721,7995142928132780707,4877213368395338842),(728,NULL,624334),(736,4199323193775611798,7421028117980622640),(739,4078105932476725481,NULL),(741,NULL,-8030356),(746,-1691668988061150727,4953),(763,14228,72),(769,5875503408282616544,NULL),(770,916090495168036187,9039593784456337022),(794,-4348345074306189013,16322),(801,-7945002,NULL),(811,96,-5979751868393457717),(816,531860,NULL),(819,7804946845868268544,NULL),(828,100,2595300248585806407),(831,-50,-332),(832,-6678277,-8304006),(835,NULL,-5708517391660480378),(837,4896762,-6425523),(838,NULL,92),(847,NULL,-4790981),(849,4484657,23),(855,76,-34),(862,-6066951284946440145,NULL),(881,-6384500018181769145,-4995),(896,6345,17888),(905,NULL,-2152246231596184366),(931,3453489499313622510,NULL),(937,-3685552750649905429,5663331292536785292),(982,7658554534507473930,8122974),(996,NULL,303808834288504749),(10,7584726391219605514,2468167924208642541),(17,6509530535377649821,-5328319169040493739),(18,49,NULL),(20,-6527598895639492262,NULL),(29,-5866415,21646),(42,-44519,-8638763694774975489),(47,NULL,6086031836504758079),(55,13461,8998339185115890014),(65,NULL,76),(80,-83,-4493687052648700102),(97,16824,NULL),(98,2978027,396260940423748282),(130,-4464201716261474000,-102),(132,NULL,1966425726719050511),(147,-8478602112084000819,NULL),(153,3041423,13299),(154,NULL,-91),(181,998037,67),(188,NULL,-3946295),(198,-69,9016921369494035032),(207,1889,-3955184002801981483),(221,7864152,122),(224,-17071,2316671),(236,-1025113713003683545,-1226340),(241,-8009722132928344996,26),(242,NULL,-1471491612676690021),(254,29,912740859434241591),(282,8643837030944926247,707775277638919669),(287,NULL,-6821544590025584605),(299,-8985253589781319746,-3874834507563132194),(301,7668472,7104930320909341814),(302,748935885669691964,5067902107495476756),(313,-9140802912354990700,5441555),(319,-7124590116659760463,-5125176200435102165),(329,-10340,78),(332,-3339673176542551819,NULL),(335,2419411,-1901056538424971252),(337,-8291,-12769),(340,424408,-8162190112821529673),(347,-2239338666097551522,-7462339164763394452),(355,5399779,-5309554096122267472),(361,NULL,-4515410),(367,-2170359324698210109,NULL),(387,700620298677679667,84),(396,2551,NULL),(413,5794128052296395518,NULL),(419,-47,6834335),(459,-22787,7789993),(488,-2217634051081839681,3281819763197679891),(499,-29542,-2029883),(501,7973466,44),(508,NULL,-1749107),(515,6925179417946177837,NULL),(536,-70,90),(549,8339263,7239652582614798447),(550,NULL,7039126),(571,-6150562,5516156392943835657),(589,-123,-1616145454488370839),(590,-3463397376201407469,8930),(592,-4978661873397300045,122),(601,3333746916756457066,-3483445),(607,93,NULL),(610,NULL,15),(629,NULL,NULL),(630,NULL,30962),(636,4023466830320315597,111),(648,2332209,NULL),(656,NULL,NULL),(676,8197104,-18),(693,1435,NULL),(697,-2584090,12695),(698,-28555,-5392951363422528831),(704,-7846786,-124),(716,103,-6481510795639224384),(724,-3960364795137656128,16074),(727,72,2939342),(733,-21593,4780608459126151856),(751,-126,-56),(759,-30,22415),(765,NULL,1071158728575403719),(766,-18452,-61),(776,6587217,392373),(779,NULL,4641444979137222268),(781,48,-3787054345370776396),(789,7034962070260565702,2706922),(808,-15917,NULL),(814,-7298406028401184882,NULL),(826,-8346979819853596875,1415739),(844,NULL,980128),(856,NULL,8227658889254292598),(859,NULL,1171863007622977054),(864,NULL,-65),(867,NULL,7815111009804780969),(869,4695916024573961629,NULL),(870,7144,NULL),(879,-27552,123),(888,-4028748,4204207056170546662),(891,-19,3286942628751182271),(894,-5402682406854330395,-784055),(908,NULL,8950571864606229113),(911,-3745966675626169780,13395),(913,-1170923455330784971,85),(914,4136454162034896486,-4070673719932479581),(926,24632,24318),(943,-8375091309258618653,-2936126218440019409),(948,-123,1610601693923998472),(951,1475910,-7297111122578779437),(961,NULL,-81),(967,-843647332522456781,NULL),(974,6851253506720470225,-8288146),(988,NULL,-7390268336797388107),(993,29270,-2797538284629322623),(5,-9,-8247024),(35,1909929,NULL),(37,-2627996488191996852,-124),(38,2349628,NULL),(41,2978379213057630453,24905),(44,3451914,NULL),(59,4642,16640),(66,6926033115934836462,-2458619),(85,11034,19875),(95,-21,-2388),(107,-69,11796),(109,8491274892812862101,2299429),(161,-3646169,NULL),(174,-2279485,213914168875414933),(182,-5436555,NULL),(195,NULL,NULL),(197,-6466256,31770),(239,1308755,5650765516391102953),(244,398668,8785367598720568954),(267,-766976,-1254807649554272221),(270,110,5889557294528025294),(281,5601878,NULL),(290,-15750,-2600442523073963023),(295,-5306785,-7840350067006497141),(296,NULL,-48),(307,-10668,-4373),(309,2235531,-1909211319961976604),(316,-8196365,NULL),(357,29620,26),(372,-15055,-3341467),(376,1621,37),(401,-112,-117),(407,1311935412221730318,385248169862200655),(416,6685056,17255),(443,38,-7131330),(446,NULL,-2383532543279797781),(449,182177155427310301,5282939133421584539),(450,-27901,8947682171473971908),(473,-6553066099932132358,121),(476,-31384,-1649731641486982129),(482,5085310725940875242,NULL),(491,3390182466930480807,-125),(522,-3347055818402203912,17951),(530,-6692831,-30823179153490681),(532,2767909414662046486,NULL),(554,4408183511012281375,-82),(559,1197815,3363082),(577,NULL,NULL),(595,-7755127,25508),(602,-7577439,-1989239408092377511),(627,-8408964045275489321,21826),(639,2286283,-117),(659,4014625,5934219403338702239),(665,941521124005766042,NULL),(666,-2213573778515267265,4609962876929807378),(668,8039422972721333548,-31799),(671,-6,-5255403509696490557),(674,4603035140417755525,618893),(679,814974268575462359,8553895561364066384),(707,4957487757813438825,-97),(719,NULL,108),(722,NULL,-31038),(775,1457566,2908605),(784,-678762,-24168),(790,91,5394162204294271583),(793,-2457,NULL),(802,13,NULL),(821,NULL,NULL),(874,NULL,-22485),(876,NULL,-11498),(882,1256028894004327657,-7286),(916,NULL,1058),(919,NULL,831),(920,-64,5952583986530345076),(928,-120,8347056),(940,NULL,-107),(946,NULL,-83),(957,7526555572834980057,102),(968,-838669,NULL),(973,52,NULL),(979,-25004,NULL),(981,546252829378776336,-35),(991,-2892979981845178245,-3940552871761534102),(994,-11489,-1991206822210877686),(999,22348,2405614249297288825),(2,NULL,28171),(25,-3656190094982092092,30409),(40,-3748199547839907485,5503890541345078597),(46,84,NULL),(53,-128,-61),(54,-8269168781827786430,-5102),(82,4184,-50),(96,-6434483,-31),(119,110,47),(133,2155665551155817753,22253),(140,25166,6343052151579052647),(145,-2286557251729634997,6297902659931381317),(155,6393561532387656406,4726408),(158,NULL,6661470),(169,NULL,3727251999414877243),(178,-7299845225536788492,21190),(200,-6929922433097756798,8358884),(203,22526,14517),(231,7588,-1262011),(237,NULL,2832751343332662641),(245,-527,NULL),(256,5618873835904861978,NULL),(261,1773,-7646808),(279,-4560938074136143129,-1190688719641091658),(288,-94,-2657490),(297,22708,-1241),(306,-17789,-971308743185799464),(315,837918066574711347,63),(318,6386604,-27872),(327,NULL,17748212270502268),(328,16125,124),(339,78,4752),(353,61,5645014),(359,3972305887111991089,-6361675409207159253),(386,24216,NULL),(410,3374701,41),(441,4279451380111775243,12188),(442,57,-97),(444,-4327137168635641650,-8970007877007216112),(448,-22,-26262),(453,112,NULL),(454,NULL,-7126072071915141261),(457,12132,NULL),(458,933316,-2164104),(463,5158519217548931603,30258),(478,-421102480438674427,92),(486,5174048,NULL),(489,NULL,-6825750909146378675),(490,6134673,-28949),(495,2879165,NULL),(507,3961344,-8114401),(528,-14795,-5887313510192644320),(531,65,NULL),(548,-8736,24541),(555,93,NULL),(565,-1151975420820005234,26809),(581,57,-30335),(591,-2126352,-95),(608,-373415,-45),(628,NULL,NULL),(635,-7861273,6414378702125981026),(643,2578260142717909518,-2112977388786723057),(649,-17,NULL),(652,-93,4696023757590112756),(655,NULL,6090130961852809314),(663,-101,4266023271879213191),(667,2246060166515030806,NULL),(677,-72,9009112869992696950),(687,-1958481392504462618,NULL),(692,-9164780234680340708,1818044309241700768),(720,NULL,3239412815429093077),(726,-5671180041146395313,21747),(732,NULL,NULL),(735,5558417,6170),(747,22312,1256),(757,-6878661753962454976,4734744),(762,NULL,-7435992397167971603),(786,5260231462483745125,-6589740),(788,11531,-19860),(791,44,8323394),(792,38,1198556203747026211),(809,NULL,-5442565824474992791),(818,NULL,24259),(820,1105463832750467847,-28874),(830,-20537,NULL),(840,-2970386359172329133,-4768150),(843,18368,-8069632),(871,27020,43),(877,-6191297629078889893,NULL),(892,-3609,NULL),(904,NULL,16650),(910,-14615,NULL),(921,-487712520551325843,128225658329108826),(922,5283532537730025607,-8646942128987133587),(929,NULL,7256235176355115076),(954,-807436,-4862882),(966,-48,-6642718189949321798),(970,-3304,-102),(990,-7549790081492622316,-2459063),(0,4111815540043927035,8085129),(9,3693292453600931913,-7426979266376293294),(52,NULL,-11145),(62,-88,-2767242),(68,-5639,-5524433459173788420),(71,NULL,687603),(75,464511926649421669,-62),(77,-12654,2836401432842654844),(83,-7351405255002120977,-90),(86,NULL,21),(89,-6132322,-7957101103606180165),(100,-24324,-1477125498852936266),(104,NULL,1426487),(112,3771373101009182262,1540198),(115,33,31406),(122,-2192908,-4795289696312410622),(127,NULL,-7984346073659404814),(129,52,NULL),(135,6871757,NULL),(171,14361,702),(191,-3521649179954079196,6526012170845080698),(215,-303071030315791859,-6723),(216,103,-6684663),(219,-7094035970782337573,28862),(222,NULL,-8000855088777995780),(227,2897786541049681539,-12679),(233,30489,NULL),(248,13,16426),(263,-5858187280427613017,-17423),(264,2730425,-76),(272,5398964081755208111,6044705681410172266),(275,-8942018056968670103,17363),(278,-1518409288582470005,-30925),(310,-252074049178343128,9349),(320,NULL,-1592322),(323,-181989186606245880,1059339339137376353),(326,-1577358560474870181,NULL),(344,-7171659679753465749,NULL),(349,NULL,NULL),(369,-7972874,-6211326),(370,66,79),(381,4858,-7590430),(384,-2680331345002790991,NULL),(402,-4883753472510552397,3481679298690027286),(408,-6885173383833547854,-8208893090021618106),(426,39,5924446947522301132),(428,-5411961,872810384978979206),(440,7629036500770650480,20084),(452,-906844,-6209893189280643105),(479,-6074359000588882854,-94),(481,NULL,3405553967983271947),(487,-5709599122578883108,11245),(502,-16522,-5279651),(511,8128593733912771104,-19063),(535,-1364995919787189202,-4785760193186387976),(541,NULL,-5189444),(542,49,51),(544,31,-8633665699397962694),(556,23552,NULL),(564,-90,-20145),(568,-20613,6065051852821863500),(583,NULL,87),(612,124,NULL),(615,-2941252221325060670,64),(616,NULL,-353417),(619,-1423854028539795071,5134303457879623225),(624,-34,-8591711174510447295),(633,-30557,-3749108),(634,-6573892,-7097524521375559702),(644,1966,12),(662,-7507281163587916613,-3966416),(685,-4615,71),(688,-5949214781242049125,14035),(691,-5209382,99),(700,-1476865436961273798,8277735),(703,8533268123187836105,8194580057868554306),(710,-3849102395457533547,31447),(731,-79,-4192361969424990412),(742,6984059,-21),(745,2813025,-7766186139988493349),(748,285017,NULL),(753,15794,874771667139909604),(756,6155168821955677687,-59),(772,-17618,63),(796,-1610039,NULL),(804,-14753,7915819),(841,-821512547235601029,NULL),(852,-60,5309),(861,-67,NULL),(884,-456837439169958159,-5932727383604993033),(887,NULL,NULL),(893,-2896160680485931234,-7263986),(899,10965,-47),(901,18508,2103677822818945230),(906,-3126000,69),(933,-5158152299362784403,2219668354459551316),(934,29833,-4871405361094207703),(952,-6247675,-515855442808708507),(964,NULL,NULL),(971,5,-30032),(976,-4800430,NULL),(4,-3931887103089601675,-5812000168930348278),(19,-384485024054923633,1753078931045546584),(26,23794,NULL),(57,NULL,-5016222164692095479),(63,-7339295091776859963,-7938110999951928377),(64,8591602329888756160,6),(67,NULL,-2588029),(81,31886,NULL),(88,2888063,NULL),(99,3482512,-51),(101,-9100517790851301511,NULL),(113,4904865,NULL),(116,-640270380985407803,-6276420),(131,-753091619392669232,-5483459729580460905),(137,-2863391965642466819,-46),(146,NULL,NULL),(148,-15385,1134004502222994811),(151,-5173026,-8005709905089683810),(152,NULL,-4856),(165,-11895,108),(172,65,NULL),(184,124,-26917),(190,8052751817808907826,NULL),(211,4661996449074067775,2842013773797377298),(223,-1090761,7102687439113490311),(226,-3289784,NULL),(240,83,5843),(243,66506976084747580,-9347),(255,-549965379048371586,-1498708452930729977),(259,6796906250774503034,8307988),(271,8635889924323584913,6892249318430708438),(291,5173719,358228050546343499),(317,3932505505624009752,-3863151155613135444),(324,32,NULL),(331,3584,4059412094801260828),(334,17584,-4578113528369028198),(342,14435,NULL),(345,-4645750154548480832,NULL),(351,-3566749,89),(365,-26,-6411),(368,-2807688,3767610),(373,44,-7190591715827938648),(388,NULL,7275),(420,4287564,3404399046005142924),(430,6850874,4),(437,-3576511654891528168,-5839888925440092245),(460,-6942021,3117725495314230835),(466,-19788,-1903446577535696513),(485,3,116),(498,7945541,-12434),(513,-2279,NULL),(520,-78,NULL),(525,1330443,-15346),(526,874001368005184751,7194282),(538,NULL,-8737019690509553036),(551,NULL,3374897402739486263),(552,-4077690737150866138,49),(560,-8377517817149662783,704061339418835366),(563,NULL,83),(569,NULL,NULL),(575,-6072838,1323651332771913044),(576,24,1247596521788677414),(593,20262,-1523934208783695934),(596,19827,NULL),(606,-1503,-7093768777424189068),(620,NULL,2994813974863066652),(645,-6108654463761732268,41),(650,859102966614210074,2419418321794102856),(657,5342059439170500130,-109),(669,4433580851321889731,NULL),(672,61,-3045363),(678,-76,14866),(689,-8440807050962240371,-119),(695,7469359,117),(718,1803589052733522633,-7239697),(723,-22800,-2202171),(738,-59,-5402492267089758391),(740,-3496556,-1975552),(758,6633229662328973423,-8915742411819657406),(771,-28092,-501689519072062584),(773,4,-64),(778,-49,-8111292327362428982),(803,1523140,-92),(806,8420,22570),(810,NULL,NULL),(812,5449679474300647136,1327674),(815,4213516,2569536),(817,-2156427761069226553,1057668384468729898),(839,NULL,-7029137857768150330),(868,NULL,9081510617318852344),(872,5494946067283635941,7787764831438909235),(890,-17144,114),(915,NULL,NULL),(939,3326096054977737078,-4903728),(942,7835703329494457090,7990597485356659900),(944,NULL,853743),(950,NULL,24),(956,1371429113901830499,2736763932039229471),(960,-154865,NULL),(969,7134752816183335915,-977189201074403522),(977,95113,NULL),(984,5823111,4946350),(997,7916220,NULL),(8,5101318286519021398,6106775),(14,-5493389,-217814),(16,-3222893433467889260,NULL),(21,5359986120838631677,-16019),(22,-5691,-10607),(31,21,7582518432817006423),(33,-2615373,2675655157761445762),(34,1762479079353341529,-12340),(48,-3513302,-5439284087794055294),(73,-6023279181224294374,22975),(76,-3743398159856895673,-117),(84,-7734606586197398923,NULL),(94,7288905,6817607),(106,4115950009674914438,-1667751889288961743),(108,-2129251568876445290,-61),(120,-8085625,-2891654),(143,-10931,NULL),(160,-44,NULL),(177,17140,-32),(180,28393,-782623965324753119),(183,7298413,NULL),(193,3048057016133781712,7509352837011696690),(206,-18290,-4758166139239598072),(208,31036,NULL),(214,-65,NULL),(220,4540899,NULL),(225,-19805,NULL),(228,NULL,4110125877452766131),(232,-3364648,1631677345239857533),(249,-5087709,-6538490537746530818),(250,-18147,-48),(262,NULL,NULL),(268,-116,23421),(274,41,123),(285,-7129603271824146514,6431375),(294,NULL,-6342163705907165881),(300,-2145658827575088107,NULL),(303,NULL,NULL),(312,7791766,58),(346,NULL,-4117960514664592199),(348,-43,21695),(363,-5360331,-4430471200094294884),(366,-469901,3826248794611497693),(385,65,-554056),(392,6040301106917674637,2342310499943339164),(397,43,-7941200788302328539),(429,-5636027303656445893,6991093),(432,-5956276,NULL),(447,-1849906,NULL),(451,6355855541589389487,NULL),(471,14,-3925548188353229853),(475,-7443770,-7983411),(480,NULL,-121),(483,3301493867582981702,517174994880179659),(516,-7551633,7739639090468795435),(519,3925980781863456300,5896270),(523,4092505972360504411,-41),(540,3541640,-4254187),(546,-8629310699690472819,7690272214148349542),(557,-2404912802398426683,22193),(588,NULL,-3055089333756836395),(594,3266539074159303742,2452498),(617,-1301326614920685131,NULL),(618,2949974086785997693,48),(623,-2,8831392486900658904),(626,-2099950707561625221,NULL),(664,-5870753,NULL),(670,6107,-8188395),(682,NULL,8238593),(699,4162422,-14684),(725,-27277,-2508),(730,7430,8517850305265278456),(744,-2263138478370096128,NULL),(750,NULL,7839119),(755,-8498248735492430388,5710673502616107521),(761,-8060253782878893995,-2835730775288417257),(767,NULL,1733217535285280781),(768,115,-3),(800,2726916,735551229436305501),(805,NULL,2145865776339616215),(827,-7262,-90),(833,3962613,NULL),(834,-5539803088705391836,-3312649576750410759),(845,21,NULL),(848,-6113085,7761907218999018126),(851,2271644,NULL),(853,3862221010123412166,-50),(854,NULL,1530305003311176873),(857,13,305667466429405666),(860,-3688466260434187982,6018303),(865,626311431408092433,-91),(880,7835670,-11634),(885,3361,NULL),(886,3196,-341001939843852296),(898,4969696225135338155,-5511256225152745746),(917,NULL,NULL),(927,75,125),(932,-617785904669739739,95),(947,7729,-3124572004468002537),(949,95,9117118440965085428),(953,-6175124363808799396,NULL),(965,NULL,NULL),(972,-2105922759977938281,21895),(978,-4247346,30155),(980,4135302856517213901,9109572558070260701),(987,NULL,-11841),(989,61,6819461),(998,-6220737771190384788,NULL),(11,6593886403872375119,-16511),(13,3109167239027650377,5390321295285305810),(28,-24840,100),(36,NULL,-6408980843302191920),(43,-126,NULL),(51,4265083,8852142350798772903),(60,-3058683020495786354,NULL),(70,-55,2397),(74,-75,156030454139134730),(79,2004999601618816405,-583932755116454381),(114,-5668457783037393310,1636864),(128,-6151461,30868),(138,NULL,-7335917688405253750),(163,95,-5936953849760482136),(170,NULL,-21762),(187,60,-85),(189,-121,NULL),(194,2649086955813120290,-452674900134009639),(213,5431543,NULL),(217,8328645474847533949,NULL),(235,-94,1701112),(238,7994897363707629441,2455193),(252,-99,NULL),(266,-636338947658804332,-6574370972353623087),(273,3399841331481191930,1187728914747827866),(276,2448579,5182583850274274462),(280,NULL,-88),(283,6242010,6276414414749547409),(286,64688392076899328,NULL),(293,NULL,-6392053),(305,6466493300255959331,29),(311,NULL,4614926),(321,-19,6394402832353128048),(333,-4940027135186387555,NULL),(336,-31723,-3107114),(354,1417256969460282874,-27583),(356,73,17116),(360,-7100427797971946112,6420910521568845647),(377,1212643,1001929),(380,6221504,-64),(383,12866,-12564),(395,-8072245041343644390,-4005003078678267710),(400,29826,191792257504749093),(405,1812308824776809386,111),(409,8620771597253240666,NULL),(415,NULL,-5286899337188580178),(417,NULL,NULL),(418,-22,-2735429332768127055),(423,-1232699246386428688,-17346),(424,-26616,84),(427,1906989094204981041,538708664883812526),(465,-7510487689170731662,-93),(468,-3361074,-3022257106871145202),(500,-3774683245205225016,NULL),(503,5814915496907384294,NULL),(534,NULL,-110),(545,29,NULL),(566,-5126425334831146422,48),(570,-8989,NULL),(599,84,119),(600,-6646754,4226801443964164083),(605,993416606925324547,-7629165),(614,NULL,-8422993161921369978),(632,26,NULL),(637,6340059197405004080,37),(638,2219025844682861975,-9181477340794568463),(640,8703206720597762646,-2467944),(658,4564753,-5711632628628133377),(675,7200328330890720300,NULL),(681,NULL,-2138727),(684,24109,10781),(696,-83,104),(705,-507277,NULL),(708,-422,-20919),(711,6420852753839600494,-253548),(712,-11386,-8748934422715143027),(717,-1244366,-2622781283346094733),(729,-2785524705743908777,946),(752,4226304450905757021,-89),(777,-34,3949395314502958079),(780,-6183873771127917812,6),(783,NULL,-27038),(785,NULL,18084),(797,7165227569808679512,9086183973837245793),(824,NULL,7837168),(829,-4019,8693244640059699436),(836,-4326053640378667417,NULL),(846,-20,NULL),(863,-6971126357652283746,1608289151182385306),(866,-7272848223721510499,-3734731507169483271),(875,NULL,-86),(895,3575,8979114980730519384),(897,1066324201229535054,-8881870179956783400),(900,-9137449359837863640,59),(907,-127,-26),(909,NULL,7307),(912,NULL,NULL),(918,-31299,8123726555850609990),(924,-7473599114166575834,8509366320847652317),(936,1091299238574024073,-8659602183742129639),(975,NULL,-1376426860463547942),(992,6920936140426763341,125),(3,-17502,-95),(6,NULL,-5683413993623134550),(15,8363285885828694100,2082650),(24,16827,4),(30,-1099629164736334542,5627737),(49,5468397,8230743511360080890),(56,1,NULL),(61,-35,-4087),(72,6236410,-18721589225916857),(78,109,-6462502474718655676),(92,NULL,2737271036229952551),(117,75,79),(118,-5547888431972866290,-60),(124,NULL,-3502233152353710671),(139,-318858,-6145754),(144,NULL,-8081314284157635734),(162,NULL,-4007796119574596450),(185,24,6092784220403498333),(186,NULL,-1925856382609852356),(192,NULL,1793501792072506064),(202,NULL,6584),(204,8801039723827585466,1),(210,-27601,-7479594981745120479),(212,-19929,NULL),(230,-124,3081827),(234,-5638401233682557873,-2972958),(247,NULL,-17384),(251,-17963,-17493),(253,NULL,431994),(258,3811692031303816498,-8980399418514755466),(260,-2162298,3111495764270491971),(269,1240606550222638489,-2025089919332743348),(284,NULL,-18028),(289,7840072496645669499,94),(292,-24068,NULL),(304,-2509251131080275613,NULL),(325,-5307406235636027683,-2000830512981277932),(330,4693318918861754777,-28052),(338,-47,-940286411604413543),(350,NULL,-2),(364,2764581187917452461,-2936323506157834634),(393,NULL,NULL),(399,-6380694531413692838,83),(411,NULL,NULL),(421,8067341,4237809),(422,4406443631050203521,-2897),(425,NULL,8502),(436,-6647982904353318197,-67),(461,-15865,-2475591754437600283),(462,43,NULL),(464,NULL,100),(469,-35,7531),(474,1729632703944492122,-414181),(493,-8868224922750828162,8275),(512,-6626760,12915),(517,5128,-87),(518,NULL,13155),(521,82,-4710064576991418898),(539,-1375268,-4843085683459073436),(578,-831682,6763301871997841994),(580,-3688788363461668115,NULL),(597,2980694444701422663,-9174131947691445728),(604,7727609030398481542,71),(609,-117,-31169),(622,234855394259543210,-8929516928448387753),(642,-3136863043231674436,NULL),(651,-2992469382270864975,NULL),(654,-1040676426731775778,-52),(661,-13123,-858),(680,-8459283889233837633,NULL),(683,-21,4748),(694,NULL,4182044146860417311),(734,-704713319638119722,8724468042514057496),(754,-6749223,-9),(760,5822749402259818364,NULL),(782,-8327928,8712801357888227090),(787,NULL,-8782409205836577879),(799,-52,7489470),(807,-11721,NULL),(813,NULL,NULL),(822,2917776,-5540580),(825,NULL,NULL),(842,-8663028079322341263,8529054945093469750),(850,-59,27),(873,-7420016877487330561,3063312),(902,24249,-242451),(923,-54,-2013158759380653631),(925,-8,-5838798365711545535),(938,25829,-6116),(945,-19,NULL),(955,-7043,-2666830601791651688),(958,-7699885015045149322,7342680),(962,7810427825411411021,-3301282690172279938),(985,NULL,NULL),(986,449502,-16);
    """
    qt_sql """
        SELECT  pk AS C1   FROM table_1000_undef_undef2 AS T1 WHERE `col_bigint_undef_signed` NOT IN (SELECT  `col_bigint_undef_signed2` AS C2   FROM  table_1000_undef_undef2 AS T2  ) OR `col_bigint_undef_signed2` <> 9 order by 1;
    """
}