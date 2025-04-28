import os

# Get the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Check if SONaa.py is in the same directory as the script
if "SONaa.py" not in os.listdir(script_dir):
    
    # Change working directory to parent of script directory
    os.chdir(os.path.dirname(script_dir))
    
    # Now check the new working directory
    current_dir = os.getcwd()
    
    # Check if SONaa.py is here
    if "SONaa.py" not in os.listdir(current_dir):
        print("SONaa.py not found in parent directory either")



from SONaa import SONaa


sonaa = SONaa()

sonaa.legacy_load_authors("main_dataset/selected_authors.csv")

sonaa.legacy_import_alternative_names({"Małgorzata_Iwona_Draps_G3l" : "Małgorzata Wordecha", 
                                "Maciej_Sekerdej_g9J" : "Maciek Sekerdej",
                                "Aleksandra_Rabinovitch_ZpX" : "Aleksandra Niemyjska",
                                "Katarzyna_Alicja_Pisańska_Rwx" : "Katarzyna Pisanski",
                                "Magdalena_Teresa_Kerestey_7d2" : "Magdalena Rydzewska",
                                "Joanna_Weronika_Płotnikowska_3M9" : "Joanna Jakubowska",
                                "Agnieszka_Dynak_DoN" : "Agnieszka Kacprzak",
                                "Kalina_Kosacka_JA4" : "Kalina Łukasiewicz",
                                "Elżbieta_Szpakiewicz_yT8": "Elżbieta Ślusarczyk",
                                "Katarzyna_Hamer-Den_Heyer_9Wc": "Katarzyna Hamer"
}, is_csv = False)

sonaa.legacy_load_orcid_articles("Step_2_Orcid/raw_article_list.csv")

sonaa.legacy_load_pbn_articles("Step_2_PBN/pbn_manually_corrected.csv")

sonaa.clean()

# merging articles with similar titles
sonaa.merge_articles(["10-5604_01-3001-0013-0638", "6785727624659994341"])
sonaa.merge_articles(["10-5114_pg-2018-81081", "10-1093_ecco-jcc_jjy222-771"])
sonaa.merge_articles(["10-3389_fpsyg-2017-00541", "10-3389_fpsyg-2016-01320"])

# merging articles with same title, but different dois
# 1
sonaa.merge_articles(["10-3389_fnhum-2017-00273", "10-3389_2ffnhum-2017-00273"])
# 2
sonaa.merge_articles(["10-13075_ijomeh-1896-00-01187", "10-13075_ijomeh-1896-01187"])
# 3
sonaa.merge_articles(["10-1016_j-ijpsycho-2017-04-006", "10-1016_j-ijpsycho-2017-04-006-", "3396143042097463437"])
# 4
sonaa.merge_articles(["10-1016_j-ijintrcl-2020-08-007", "10-1016_j-ijintrel-2020-08-007"])
# 5
sonaa.merge_articles(["10-1080%2F02680939-2019-1576923", "10-1080_02680939-2019-1576923"])
# 6
sonaa.merge_articles(["10-3758_s13415-018-0578-4", "10-3758_s13415-018-0578-4-"])
# 7
sonaa.merge_articles(["10-1007_s10484-018-9426-0", "10-1007_s1048"])
# 8
sonaa.merge_articles(["10-13075_ijomeh-1896-01406", "10-13075_ijomeh-1896-1406"])
# 9
sonaa.merge_articles(["10-1016_j-jcrimjus-2020-101750", "10-1177_1359104520969765"])
# 10
# merged elier
# 11
sonaa.merge_articles(["10-7366%2F1896180020174006", "10-7366_1896180020174006"])
# 12
sonaa.merge_articles(["10-1504_ijwoe-2021-115620", "10-1504_ijwoe-2021-10037977"])
# 13
sonaa.merge_articles(["10-19195_fs-19-2-172-189", "10-34616_fs-19-2-172-189", "6280112080724089389"])
# 14
sonaa.merge_articles(["10-4467_20843879pr-20-009-12265", "10-4467_20843879pr-20-012-12267"])
# 15
sonaa.merge_articles(["10-1097%2Fwnn-0000000000000183", "10-1097_wnn-0000000000000183"])
# 16
sonaa.merge_articles(["10-3828_rs-2019-3", "10-3828_rs-2019-03"])
# 17
sonaa.merge_articles(["10-1371_journal-pone-0200604", "10-1371_journal-pone-0200604- ecollection 2018"])
# 18
sonaa.merge_articles(["10-3389_fpsyg-2019-00281", "10-3389%2Ffpsyg-2019-00281"])
# 19
sonaa.merge_articles(["10-1515_ppb-2017-0026", "10-1515_ppb-2017-0026-"])
# 20
sonaa.merge_articles(["10-5964_spb-v13i1-25502", "10-5964_spb-v-13i1-25502"])
# 21
sonaa.merge_articles(["10-5406_amerjpsyc-132-1-0057", "10-5406%2Famerjpsyc-132-1-0057"])
# 22
sonaa.merge_articles(["10-5593_sgemsocial2018_3-3", "10-5593_sgemsocial2018_3-3_s12-067"])
# 23
sonaa.merge_articles(["10-5593_sgemsocial2018_3-2", "10-5593_sgemsocial2018_3-2_S11-070"])
# 24
sonaa.merge_articles(["10-14691_cppj-23-1-43", "10-14691_cppj-23-1-4"])
# 25
# Not merging conflict #25 "Introduction" articles with different authors
# 26
sonaa.merge_articles(["10-3389_fpsyg-2020-00553", "10-3389_fpsych-2020-00553"])
# 27
sonaa.merge_articles(["10-25167%2Fff%2F2017%2F127-143", "10-25167_ff_2017_127-143"])
# 28
sonaa.merge_articles(["10-1080_00221309-2018-1543646", "10-1080_002213092018-1543646", "56189960158818383"])
# 29
sonaa.merge_articles(["10-1007%2Fs12144-019-00185-1", "10-1007_s12144-019-00185-1"])
# 30
sonaa.merge_articles(["10-1177_0886260515603 974", "10-1177_0886260515603974"])
# 31
sonaa.merge_articles(["10-15561_20755279-2020-0101", "10-15561_20755279-2020-0107"])
# 32
sonaa.merge_articles(["10-12775_17580", "10_12775&jpm_2017_131"])
# 33
sonaa.merge_articles(["10-18290_PEPSI-2021-0007", "10-19290_pepsi-2021-0010"])

print("merging ends")
sonaa.legacy_load_openalex_articles("Step_2_OpenAlex/openalex.csv")
sonaa.clean()

# merging articles with same title but different DOIs
sonaa.merge_articles(["10-3389_2ffnhum-2017-00273","10_3389&fnhum_2017_00273"])
sonaa.merge_articles(["10_13075&ijomeh_1896_00_01187","10-13075_ijomeh-1896-01187"])
sonaa.merge_articles(["10_1016&j_ijpsycho_2017_04_006","10-1016_j-ijpsycho-2017-04-006-","3396143042097463437"])
sonaa.merge_articles(["10-1016_j-ijintrel-2020-08-007","10_1016&j_ijintrcl_2020_08_007"])
sonaa.merge_articles(["10-1080_02680939-2019-1576923","10-1080_02680939-2019-1576923"])
sonaa.merge_articles(["10_3758&s13415-018-0578-4","10-3758_s13415-018-0578-4-"])
sonaa.merge_articles(["10_1007&s10484-018-9426-0","10-1007_s1048"])
sonaa.merge_articles(["10_13075&ijomeh_1896_01406","10-13075_ijomeh-1896-1406"])
sonaa.merge_articles(["10_1016&j_jcrimjus_2020_101750","10-1177_1359104520969765"])
sonaa.merge_articles(["10_3389&fpsyg_2017_00541","10-3389_fpsyg-2016-01320"])
sonaa.merge_articles(["10-7366_1896180020174006","10_7366%2f1896180020174006"])
sonaa.merge_articles(["10-1371_journal-pone-0256430","10_5281&zenodo_5235181"])
sonaa.merge_articles(["10-1016_j-psychsport-2019-101584","10_5281&zenodo_4550850"])
sonaa.merge_articles(["10-1007_s12144-017-9629-1","10_5281&zenodo_4550861"])
sonaa.merge_articles(["10-1504_ijwoe-2021-10037977","10_1504&ijwoe_2021_115620"])
sonaa.merge_articles(["10_19195&fs_19_2_172_189","10-34616_fs-19-2-172-189", "6280112080724089389"])
sonaa.merge_articles(["10_4467&20843879pr_20_009_12265","10-4467_20843879pr-20-012-12267"])
sonaa.merge_articles(["10-1097_wnn-0000000000000183","10_1097%2fwnn_0000000000000183"])
sonaa.merge_articles(["10-3828_rs-2019-03","10_3828&rs_2019_3"])
sonaa.merge_articles(["10-26417_ejis-v9i1-p73-80","10_26417&ejis_v3i4_p73-80"])
sonaa.merge_articles(["10_1371&journal_pone_0200604","10-1371_journal-pone-0200604- ecollection 2018"])
sonaa.merge_articles(["10_3389&fpsyg_2019_00281","10-3389%2Ffpsyg-2019-00281"])
sonaa.merge_articles(["10_1515&ppb-2017-0026","10-1515_ppb-2017-0026-"])
sonaa.merge_articles(["10-5964_spb-v-13i1-25502","10_5964&spb_v13i1_25502"])
sonaa.merge_articles(["10_1080&00223980_2019_1581723","10-4324_9781003243601-4"])
sonaa.merge_articles(["10_5406&amerjpsyc_132_1_0057","10-5406%2Famerjpsyc-132-1-0057"])
sonaa.merge_articles(["10-5593_sgemsocial2018_3-3_s12-067","10_5593&sgemsocial2018&3_3"])
sonaa.merge_articles(["10-5593_sgemsocial2018_3-2_S11-070","10_5593&sgemsocial2018&3_2"])
sonaa.merge_articles(["10_14691&cppj_23_1_43","10-14691_cppj-23-1-4"])
# 30 introductions
sonaa.merge_articles(["10_3389&fpsyg_2020_00553","10-3389_fpsych-2020-00553"])
sonaa.merge_articles(["10-21697_spch-2018-54-3-13","10_21697&2018_54_3_13"])
sonaa.merge_articles(["10-25167_ff_2017_127-143","10_25167%2fff%2f2017%2f127-143"])
sonaa.merge_articles(["10-1080_10615806-2018-1475865","10_1080&10615806_2018_1475868"])
sonaa.merge_articles(["10_1080&00221309_2018_1543646","10-1080_002213092018-1543646", "56189960158818383"])
sonaa.merge_articles(["10-1007_s12144-019-00185-1","10_1007%2fs12144-019-00185-1"])
sonaa.merge_articles(["10-31261_ijrel-2018-5-2-04","10_31261&ijrel_2019_5_2_04"])
sonaa.merge_articles(["10-1016_J-COGNITION-2021-104698","10_1167&jov_21_9_2117"])
sonaa.merge_articles(["10-1177_0886260515603974","10_1177&0886260515603 974"])
sonaa.merge_articles(["10-15561_20755279-2020-0107","10_15561&20755279_2020_0101"])
sonaa.merge_articles(["10-47577_tssj-v9i1","10_47577&tssj_v9i1_907"])
sonaa.merge_articles(["10_3390&ijerph17186491","10-20944_preprints202008-0176-v1"])
sonaa.merge_articles(["10_12775&jpm_2017_131","10_12775&17580"])
sonaa.merge_articles(["10-19290_pepsi-2021-0010","10_18290&pepsi-2021-0007"])
sonaa.merge_articles(["10_3791&60804","10_3791&60804-v"])
sonaa.merge_articles(["10_5281&zenodo_1039366","10_5281&zenodo_1040684"])

# merging similar title pairs:
sonaa.merge_articles(["6785727624659994341","10_5604&01_3001_0013_0638"])
sonaa.merge_articles(["10_13174&pjambp_19_06_2018_02","4877423162047736766"])
sonaa.merge_articles(["10-5114_pg-2018-81081","10_1093&ecco-jcc&jjy222_771"])
sonaa.merge_articles(["10-17554_j-issn-2309-6861-2018-05-139","-6169101500242423537"])
sonaa.merge_articles(["10-26417_ejes-v4i2-p35-40","10-2478_ejes-2018-0036", "9079397821835399471"])
sonaa.merge_articles(["10-2478_ejes-2018-0041","10-26417_ejes-v4i2-p83-89"])
sonaa.merge_articles(["10-1007_s10803-019-04253-0","10-1007_s10803-019-04303-7"])
sonaa.merge_articles(["10-34766_fetr-v46i2-792","-5581247057351592303"])
sonaa.merge_articles(["10-18778_1427-969x-21-06","-71253364286370115"])
sonaa.merge_articles(["1142411137119413016","-6099408006679730598"])
sonaa.merge_articles(["10-1177_0084672420983488","7856750526515606812"])
sonaa.merge_articles(["10-3390_nu12030646","-7750630605998233900"])
sonaa.merge_articles(["10-1016_j-intell-2018-07-003","10-1016_j-intell-2018-11-005"])
sonaa.merge_articles(["10-3389_fpsyg-2018-01081","10-3389_fpsyg-2019-01860"])
sonaa.merge_articles(["10-1504_ijwoe-2021-120717","-1447205995660454754"])


print("merging ends")

sonaa.update_files("private_data/publications")