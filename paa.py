import os
import sys	
from time import sleep
from multiprocessing import Process, Manager
from arcpy import env, CheckOutExtension, CheckInExtension, CopyFeatures_management, AddMessage
from arcpy.sa import CellStatistics, Float, Aggregate, Con
from collections import namedtuple
osoite = os.path.dirname(os.path.realpath(__file__)) #r'D:\Aineistot\test_trkj\_FME_Mikko_Kesala\Thomas_tiedostot\Digiriistametsa_1_1_6_4\paa.py'
sys.path.append(osoite)
import funktiot

with open('{0}\\attribuutit.txt'.format(osoite), 'r') as tied:
	attriLista = tied.read().split(';;')
paaOsoite = attriLista[1]
valiOsoite = '{0}\\vali\\'.format(paaOsoite)
env.scratchWorkspace = env.workspace = valiOsoite
CheckOutExtension('Spatial')
env.overwriteOutput = True
env.snapRaster = env.extent = attriLista[4]
if not os.path.exists(valiOsoite): os.makedirs(valiOsoite)

# Jaetaan tehtavat ytimille
def ydin1(kanaNimiLis, valiPo, paaPol, krigLista, suomiMaski, kana1, kana2, tyhjaLis, aineistoLis, MTK, vesistoLis, luontoLis):
	funktiot.VMItoimenpide(tyhjaLis[4], aineistoLis)
	funktiot.krigiFunk(kanaNimiLis[0], kana1.split('\\')[-1][:-4], valiPo, paaPol, krigLista, suomiMaski)
	funktiot.krigiFunk(kanaNimiLis[1], kana2.split('\\')[-1][:-4], valiPo, paaPol, krigLista, suomiMaski)
	funktiot.merge(vesistoLis[0], vesistoLis[1], vesistoLis[2], vesistoLis[3], MTK)
	funktiot.merge(luontoLis[0], luontoLis[1], luontoLis[2], luontoLis[3], MTK)

def ydin2(tyhjaLis, aineistoLis, tasLis1, tasLis2, valiPo, paaPol, MTK, ihmisRaLi, IhmisLinLis, zonaNimi, listatut):
	funktiot.VMItoimenpide(tyhjaLis[1], aineistoLis)
	funktiot.VMItoimenpide(tyhjaLis[0], aineistoLis)
	funktiot.VMItoimenpide(tyhjaLis[2], aineistoLis)
	latvus = funktiot.oikea(aineistoLis, 'latv')
	funktiot.lk_mvmi(tasLis1, latvus)
	funktiot.tiheys(tasLis2)
	funktiot.ihmiset(MTK, ihmisRaLi, listatut, IhmisLinLis, paaPol, zonaNimi)

def ydin3(tyhjaLis, aineistoLis, tasLis1, tasLis2, zonaHilaNimi, puulajienmaara = 3):
	funktiot.VMItoimenpide(tyhjaLis[3], aineistoLis)
	funktiot.VMItoimenpide(tyhjaLis[5], aineistoLis)
	tilavuus = funktiot.oikea(aineistoLis, 'tila')
	funktiot.lk_mvmi(tasLis1, tilavuus, zonaHilaNimi)
	# Varmistetaan, etta kaikki tiedostot on kasitelty. Parempaa ratkaisua odotellessa.
	while len(aineistoLis) < 6:
		sleep(10)
	manty, muulp, kuusi, koivu, tilavuus = Float(funktiot.oikea(aineistoLis, 'manty')), funktiot.oikea(aineistoLis, 'muulp'), funktiot.oikea(aineistoLis, 'kuusi'), funktiot.oikea(aineistoLis, 'koivu'), funktiot.oikea(aineistoLis, 'tila')
	maksi = (1 - (CellStatistics([manty, muulp, kuusi, koivu], 'MAXIMUM', 'DATA') / tilavuus)) / (1 - (1/puulajienmaara))
	funktiot.lk_mvmi(tasLis2, maksi)

if __name__ == '__main__':
	# Zonationiin menevat tiedostojen nimet
	suoLista = (attriLista[10], attriLista[11], attriLista[12])
	zonaRasterLista = tuple(map(lambda x: '\\'.join([paaOsoite, x]), ('tilavuus.tif', 'tiheys.tif', 'latvus.tif', 'puulajit.tif', 'hilaMaara.tif', 'luonto.tif', 'metepy.tif', 'riekko.tif', 'ihmiset.tif', 'vesistot.tif')))

	pohjaAineisto, krigiLista, = Manager().list(), Manager().list()
	VMINimi = ('tilavuus_', 'latvuspeitto_','manty_','kuusi_','muulp_','koivu_')
	kanaKokoelma = funktiot.kanaAineisto(valiPo = valiOsoite, riistaOsoite = attriLista[7],  koordit = attriLista[8], nimi1 = zonaRasterLista[6], nimi2 = zonaRasterLista[7])

	suomi = funktiot.GDBTarkistus(valiOsoite) + 'Suomimaski'
	CopyFeatures_management(attriLista[3], suomi)
	vesistoLista = (('36200', 'e_44300', '38700', '38600', '36211', '36313', '38300'), 'in_memory\\kaikkivesistot', 'vesistoraster.tif', zonaRasterLista[9])
	luontoLista = (('39110', '34300', '32800', '35411', '35421', '39130'), 'in_memory\\kaikkiLuonto', 'luontoraster.tif', zonaRasterLista[5])
	ihmisRakLista = (
				'32611', '42241', '42242', '42250', '42251', '42251', '42260', '42261', '42262', '42270'
				, '42210', '42211', '42212', '42220', '42221', '42222', '42230', '42231', '42232', '42240'
				, '38900', '40200', '32500', '32421', '32418', '32417', '32416', '32415', '32414', '32413'
				, '32412', '32411', '32300', '33000', '32113', '32112', '32111')

	suuretIhmisTiedosto = ('32611', '42211', '42231', '42261') # Listattu 4 suurinta tiedostoa "ihmisRakLista" muuttujasta
	ihmisLinjatLista = (('22311', '14111', '12122', '12112', '12121', '12111'), 'in_memory\\kaikkiviivat', 'viivarcl.tif')
	MTKAineisto = attriLista[0]
	tyhjaLista = funktiot.pohjaLista(lukuOsoite = attriLista[5] + '\\', haLista = VMINimi)

	hilaKentta = ('puulajienlkm', 'v', 'latvuspeittavyys5', 'tiheys5') # Tarkista, etta vastaa hila tiedoston kenttia
	riistaHila = attriLista[9]

	tas1Lista = (riistaHila, zonaRasterLista[3], '32_BIT_FLOAT', hilaKentta[0])
	tas2Lista = (riistaHila, zonaRasterLista[0], '16_BIT_UNSIGNED', hilaKentta[1])
	tas3Lista = (riistaHila, zonaRasterLista[2], '8_BIT_UNSIGNED', hilaKentta[2])
	tas4Lista = (riistaHila, zonaRasterLista[1], '8_BIT_UNSIGNED', hilaKentta[3])

	# Rinnakkais prosessit
	pros_ydin1 = Process(target = ydin1, args = (kanaKokoelma, valiOsoite, paaOsoite, krigiLista, suomi, zonaRasterLista[6], zonaRasterLista[7], tyhjaLista, pohjaAineisto, MTKAineisto, vesistoLista, luontoLista))
	pros_ydin2 = Process(target = ydin2, args = (tyhjaLista, pohjaAineisto, tas3Lista, tas4Lista, valiOsoite, paaOsoite, MTKAineisto, ihmisRakLista, ihmisLinjatLista, zonaRasterLista[8], suuretIhmisTiedosto))
	pros_ydin3 = Process(target = ydin3, args = (tyhjaLista, pohjaAineisto, tas2Lista, tas1Lista, zonaRasterLista[4]))

	pros_ydin1.start()
	pros_ydin2.start()
	pros_ydin3.start()

	pros_ydin1.join()
	pros_ydin2.join()
	pros_ydin3.join()

	CheckInExtension('Spatial')
	del pohjaAineisto, suomi, VMINimi, valiOsoite, MTKAineisto, pros_ydin1, pros_ydin2, pros_ydin3
	del vesistoLista, luontoLista, ihmisRakLista, ihmisLinjatLista, kanaKokoelma, tyhjaLista, krigiLista
	del tas1Lista, tas2Lista, tas3Lista, tas4Lista, suuretIhmisTiedosto, riistaHila, hilaKentta

	znTiedosto = namedtuple('Rasteri', 'painotukset tiedosto')
	kernel_10km = round(2.0 / ((1000 / 64) * 64), 5) # etaisyys / solukoko = hilojen maara. Yksikkona metri
	kernel_xkm = round(2.0 / ((x / 64) * 64), 5)
	#variantti 1
	pk1 = znTiedosto(painotukset = '2.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[0])
	pk2 = znTiedosto(painotukset = '2.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[1])
	pk3 = znTiedosto(painotukset = '2.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[2])
	pk4 = znTiedosto(painotukset = '2.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[3])
	pk5 = znTiedosto(painotukset = '2.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[4])
	#variantti 2
	pk6 = znTiedosto(painotukset = '2.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = suoLista[0])
	pk7 = znTiedosto(painotukset = '3.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = suoLista[1])
	pk8 = znTiedosto(painotukset = '1.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = suoLista[2])
	#variantti 3
	pk9 = znTiedosto(painotukset = '0.5 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[5])
	pk10 = znTiedosto(painotukset = '1.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[6])
	pk11 = znTiedosto(painotukset = '1.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[7])
	pk12 = znTiedosto(painotukset = '-1.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[8])
	pk13 = znTiedosto(painotukset = '-1.0 {0} 1 1 0.25'.format(kernel_10km), tiedosto = zonaRasterLista[9])

	#zonation
	znMask = attriLista[2]
	matriisi = ''
	interaktio = ''
	BLP = ''
	if znMask.lower() == 'none':
		znMask = None
	variaLista = (pk1, pk2, pk3, pk4, pk5, pk6, pk7, pk8, pk9, pk10, pk11, pk12, pk13)
	variaTiedMaara = (5, 8, 13)
	for x in range(len(variaTiedMaara)):
		znBatLista = (attriLista[6], paaOsoite + '\\Zona\\drm_zonation{0}'.format(x), '0.0 1 1.0 1', 'drm_zonation{0}.bat'.format(x), 'drm_zonation{0}_out\\drm_zonation{0}.txt'.format(x))
		znDatLista = ('2', '10000', '1', '0', '1', znMask, 'drm_zonation{0}.dat'.format(x))
		znSPPLista = ['drm_zonation{0}.spp'.format(x)]
		znSPPLista.extend(variaLista[:variaTiedMaara[x]])
		znBatPolku = paaOsoite + '\\Zona\\drm_zonation{0}\\drm_zonation{0}.bat'.format(x)
		if x >= len(variaTiedMaara)-1:
			matriisi = ('drm_zonation{0}_matrix.txt'.format(x), '1', '1'
						, ''
						, '0.5'
						, '0.9 0.3'
						, '0.2 0.5 0.3'
						, '0.6 0.8 0.7 0.7'
						, '0.1 0.1 0.1 0.1 0.1'
						, '0.3 0.3 0.5 0.6 0.1 0.8'
						, '0.3 0.3 .05 0.6 0.1 0.6 0.95'
						, '0.1 0.1 0.1 0.1 0.1 0.1 0.1 0.1'
						, '0.2 0.2 0.2 0.2 0.3 0.2 0.2 0.2 0.1'
						, '0.1 0.1 0.1 0.1 0.3 0.9 0.8 0.6 0.1 0.4'
						, '0.01 0.01 0.01 0.01 0.01 0.01 0.01 0.01 0.5 0.01 0.01'
						, '0.01 0.01 0.01 0.01 0.01 0.01 0.01 0.01 0.5 0.01 0.01 0.6')
			interaktio2 = """('drm_zonation{0}_interactions.txt'.format(x), '1'
						,'1 1 {0} 1 1'.format(kernel_xkm)
						)"""
			#BLP = '0.1'
		funktiot.zona(znBatLista, znDatLista,znSPPLista, znBatPolku, matriisi, interaktio, BLP)