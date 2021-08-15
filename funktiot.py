import arcpy
import os
from time import sleep
from datetime import datetime
from arcpy.sa import Con, Raster, ExtractByMask, Aggregate, Kriging, KrigingModelOrdinary, RadiusVariable, Reclassify, RemapRange, LineStatistics, IsNull
from multiprocessing import Manager
arcpy.env.overwriteOutput = True

# Pidetaan huoli, etta oikea tiedosto menee muuttujaan
def oikea(kokoelma, haettava):
	for k in kokoelma:
		if haettava in k:
			return Raster(k)

# Loytyyko geodatabase ja luodaan tarvittaessa
def GDBTarkistus(vaPo, geDB = '\\vali.gdb'):
	paikka = vaPo + geDB
	if not arcpy.Exists(paikka):
		arcpy.CreateFileGDB_management(vaPo[:-1], geDB)
	return paikka + '\\'

# Kaydaan kansio lapi ja palautetaan lista, jossa on haetut tiedostopolut
def pohjaLista(lukuOsoite, haLista):
	omaLista = []
	tiedOso = arcpy.da.Walk(lukuOsoite)
	for dirpath, dirnames, filenames in tiedOso:
		for filename in filenames:
			tied = filename.lower()
			for haku in haLista:
				if haku in tied and 'bm' not in tied and 'lehti' not in tied:
					omaLista.append(os.path.join(dirpath, filename))
	return sorted(omaLista)

# Kirjoitetaan kaytetty aika lokiin, joka sijaitsee scriptin kanssa samassa kansiossa. Liitetaan halutun funktion loppuun ja alkuun laitetaan muuttuja ja sille arvo datetime.now().
def ajastus(aika, tulosNimi, tyhjennetaan = False):
	aika_tiedosto = '{0}\\aika_tiedosto.txt'.format(os.path.dirname(os.path.realpath(__file__)))
	if not os.path.exists(aika_tiedosto) or tyhjennetaan:
		with open(aika_tiedosto, 'w') as tiedosto:
			tiedosto.write(str(datetime.now()))
	erotus = datetime.now() - aika
	with open(aika_tiedosto, 'a') as tiedosto:
		tiedosto.write('\n{0}: \t{1}'.format(funkNimi, str(erotus).split('.')[0]))

# MVMI aineistojen kasittely
def VMItoimenpide(tiedosto, niLista):
	VMInimi = tiedosto.split('\\')[-1][:6] + '_con.tif'
	rasTied = Raster(tiedosto)
	aggre = Aggregate(rasTied, 4, 'MEAN')
	Con(aggre, aggre, '', 'Value > 0 AND Value < 1000').save(VMInimi)
	niLista.append(VMInimi)

# Kasitellaan piirrekerrokset 1-4
def lk_mvmi(lista, mvmi, hilaMaaraNimi = ''):
	nimi = lista[3][:4] + '.tif'
	arcpy.FeatureToRaster_conversion(lista[0], lista[3], nimi, 16)
	aggre = Aggregate(nimi, 4, 'MEAN')
	if hilaMaaraNimi:
		coni = Con(nimi, 1, '', 'Value > 0')
		Aggregate(coni, 4, 'SUM').save(hilaMaaraNimi)
	coni = Con(IsNull(aggre) == 1, mvmi, aggre)
	if arcpy.GetRasterProperties_management(coni, 'VALUETYPE') != lista[2]:
		nimi = 'coni_kopio.tif'
		arcpy.CopyRaster_management(coni, nimi, pixel_type = lista[2])
	else:
		nimi = coni
	Con(nimi, nimi, '', 'Value > 0').save(lista[1])

# Kasitellaaj tiheys tiedostoa
def tiheys(tasLis):
	arcpy.PolygonToRaster_conversion(tasLis[0], tasLis[3], 'tiheys.tif', '', '', 16)
	aggre = Aggregate('tiheys.tif', 4, 'MEAN')
	arcpy.CopyRaster_management(aggre, 'tiheysraster.tif', pixel_type = tasLis[2])
	Con('tiheysraster.tif', 'tiheysraster.tif', '', 'Value > 0').save(tasLis[1])

# Ihmisen luoma linja, esim tiet
def IhmisLinjat(pk10lista, tyoTaso):
	ihmisLista = pohjaLista(lukuOsoite = tyoTaso, haLista = pk10lista[0])
	merge(ihmisLista, pk10lista[1])
	viivaStats = LineStatistics(pk10lista[1],'None', 64, 64, 'LENGTH')
	viivaHaku30 = viivaStats >= 30
	arcpy.CopyRaster_management(viivaHaku30, pk10lista[2], pixel_type = '1_BIT')

# Kasitellaan ihmisten aukot, joista erotellaan suurimmat tiedostot ja kasitellaan ne yksitellen
def ihmiset(MTK, ihmisRaLi, listatut, IhmisLinLis, paaPol, zonaNimi):
	rasLista, rclLista = tuple(map(lambda x: 'ras{0}.tif'.format(x), range(5))), tuple(map(lambda x: 'rcl{0}.tif'.format(x), range(5)))
	pk10Lista = pohjaLista(MTK, ihmisRaLi)
	suuret = set([(p) for p in pk10Lista for l in listatut if l in p])
	uusiLista = [set(pk10Lista) - suuret]
	uusiLista.extend(suuret)
	merge(tuple(uusiLista[0]),  '\\vali.gdb\\valiMerge', rasLista[0], rclLista[0])
	merge(uusiLista[1], uusiLista[1], rasLista[1], rclLista[1])
	merge(uusiLista[2], uusiLista[2], rasLista[2], rclLista[2])
	merge(uusiLista[3], uusiLista[3], rasLista[3], rclLista[3])
	merge(uusiLista[4], uusiLista[4], rasLista[4], rclLista[4])
	IhmisLinjat(IhmisLinLis, MTK)
	arcpy.MosaicToNewRaster_management([rclLista, IhmisLinLis[2]], paaPol, zonaNimi[len(paaPol):], '', '1_BIT', 64, 1, 'FIRST')

# Muutetaan tiedosto rasteriksi ja tapauskohtaisesti luodaan ja yhdistetaan lista
def merge(mergeLista, mergeNimi, rasteriNimi = '', rclNimi = '', maastoTietoKanta = ''):
	if maastoTietoKanta:
		mergeLista = pohjaLista(maastoTietoKanta, mergeLista)
	if type(mergeLista) in (list, tuple, set):
		arcpy.Merge_management(mergeLista, mergeNimi)
		mergeLista = mergeNimi
	if rasteriNimi:
		arcpy.PolygonToRaster_conversion(mergeLista, 'LUOKKA', rasteriNimi, 'MAXIMUM_COMBINED_AREA', '', 64)
		Reclassify(rasteriNimi, 'Value', RemapRange([[1,99999, 1]]), 'NODATA').save(rclNimi)

# Kriging toimenpide
def krigiFunk(tiedosto, kanaNimi, valiPo, talPo,  pohAi, suomi, etaisyys_m = 50000):
	kriNimi = '{0}\\{1}.tif'.format(talPo, kanaNimi)
	conimi= 'con{0}'.format(kanaNimi)
	krigi = Kriging(tiedosto, 'SUM_n', KrigingModelOrdinary('GAUSSIAN', '', etaisyys_m, 9, 2), 64, RadiusVariable(12, etaisyys_m))
	sleep(3)
	arcpy.CopyRaster_management(krigi, conimi, pixel_type = '8_BIT_UNSIGNED')
	extrac = ExtractByMask(conimi, suomi)
	Con(extrac > 0, extrac).save(kriNimi)
	pohAi.append(kriNimi)

# Valmistellaan ja eritellaan kanalinnut krigingia varten
def kanaAineisto(valiPo, riistaOsoite, koordit, nimi1, nimi2):
	riista = GDBTarkistus(valiPo) + '\\riistaPisteet'
	valiNimi = '\\vali.gdb\\valiriista'
	kanaLista = (nimi1.split('\\')[-1][:-4], nimi2.split('\\')[-1][:-4])
	arcpy.MakeXYEventLayer_management(riistaOsoite, 'kolmio_keskipiste_x', 'kolmio_keskipiste_y', valiNimi, koordit)
	arcpy.FeatureToPoint_management(valiNimi, riista)
	arcpy.MakeFeatureLayer_management(riista, 'kana_taso')
	kanaNimiLista = Manager().list()
	for k in kanaLista:
		kanaGDB = GDBTarkistus(valiPo, '\\{0}.gdb'.format(k))
		ilmaisu = 'laji_txt = \'{0}\''.format(k)
		if k == kanaLista[0]:
			ilmaisu = 'laji_txt = \'{0}\' OR laji_txt = \'{1}\' OR laji_txt = \'{2}\''.format('metso', 'teeri', 'pyy')
		if arcpy.Exists('{0}{1}_86hakutj'.format(kanaGDB, k)):
			arcpy.Delete_management('{0}{1}_86hakutj'.format(kanaGDB, k))
		uusiNimi = '{0}{1}_86hakutj'.format(kanaGDB, k)
		arcpy.SelectLayerByAttribute_management('kana_taso', 'NEW_SELECTION', ilmaisu)
		arcpy.Dissolve_management('kana_taso', uusiNimi + 'dissolve', 'knum', [['n', 'SUM'], ['kolmio_keskipiste_x', 'FIRST'], ['kolmio_keskipiste_y', 'FIRST']], 'SINGLE_PART')
		kanaNimiLista.append(uusiNimi + 'dissolve')
	return kanaNimiLista

# Kirjoitetaan zonation tiedostot
def zona(batlista, datlista, spplista, batpolku, matriisi, interaktio, blp):
	if not os.path.exists(batlista[1]):
		os.makedirs(batlista[1])

	# BAT
	with open(batlista[1] + '\\' + batlista[3], 'w') as bat_tiedosto:
		bat_tiedosto.write('call {0} -r {1} {2} {3} {4}  --use-threads --grid-output-formats compressed-tif\n'.format(batlista[0], batlista[1] + '\\' + datlista[6], batlista[1] + '\\' + spplista[0], batlista[1] + '\\' + batlista[4], batlista[2]))

	# Matriisi
	if matriisi:
		with open(batlista[1] + '\\' + matriisi[0], 'w') as matriisi_tiedosto:
			for m in matriisi[3:]:
				matriisi_tiedosto.write('{0}\n'.format(m))

	# DAT
	with open(batlista[1] + '\\' + datlista[6], 'w') as dat_tiedosto:
		dat_tiedosto.write('[Settings]\nremoval rule = {0}\nwarp factor = {1}\nedge removal = {2}\n'.format(datlista[0], datlista[1], datlista[2]))
		if interaktio:
			dat_tiedosto.write('\nuse interactions = {0}\ninteraction file = {1}\n'.format(interaktio[1], batlista[1] + '\\' + interaktio[0]))
		if datlista[5]:
			dat_tiedosto.write('\nmask missing areas = {0}\narea mask file = {1}\n'.format(datlista[4], datlista[5]))
		if matriisi:
			dat_tiedosto.write('\n[Community analysis settings]\nload similarity matrix = {0}\nconnectivity similarity matrix file = {1}\napply to connectivity = {2}\n'.format(matriisi[1], batlista[1] + '\\' + matriisi[0], matriisi[2]))
		if blp:
			dat_tiedosto.write('\nBLP = {0}'.format(blp))

	# SPP
	with open(batlista[1] + '\\' + spplista[0], 'w') as spp_tiedosto:
		for spp in spplista[1:]:
			spp_tiedosto.write('{0} {1}\n'.format(spp[0], spp[1]))

	# Interaction
	if interaktio:
		with open(batlista[1] + '\\' + interaktio[0], 'w') as interaktio_tiedosto:
			for a in interaktio[2:]:
				interaktio_tiedosto.write('{0}\n'.format(a))

	os.system(batpolku)