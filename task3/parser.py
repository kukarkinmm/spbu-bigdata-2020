import requests
import lxml.html
import lxml.etree
import numpy as np
import pandas as pd
from os import system, name


url = 'http://www.vybory.izbirkom.ru/region/region/izbirkom?action=show&root=1&tvd=100100084849066&vrn=' \
      '100100084849062&region=0&global=1&sub_region=0&prver=0&pronetvd=null&vibid=100100084849066&type=227'


def clear():
    if name == 'nt':
        _ = system('cls')
    else:
        _ = system('clear')


if __name__ == "__main__":

    response = requests.get(url)
    tree = lxml.html.fromstring(response.text)
    urls = tree.xpath('//option/@value')
    regions = tree.xpath('//option/text()')

    columns = ['Регион', 'Название ТИК', 'Номер УИК']
    columns += tree.xpath('//table/tr/td[contains(@align,"left")]/nobr/text()')
    df = pd.DataFrame()

    for i, (region, region_link) in enumerate(zip(regions[1:], urls)):

        clear()
        print(f"parsing in progress -- {i + 1}/{len(urls)}")
        print(f"current region: {region}")

        region_tree = lxml.html.fromstring(requests.get(region_link).text)
        districts = region_tree.xpath('//option/text()')
        district_links = region_tree.xpath('//option/@value')
        for district, district_link in zip(districts[1:], district_links):
            district_tree = lxml.html.fromstring(requests.get(district_link).text)
            polling_stations_link = district_tree.xpath('//table/tr/td/a/@href')[3]
            polling_stations_tree = lxml.html.fromstring(requests.get(polling_stations_link).text)
            table = polling_stations_tree.xpath('//table/tr/td[contains(@align,"right")]/nobr/b/text()')[20:]
            polling_stations = polling_stations_tree.xpath('//table/tr[contains(@valign, "top")]'
                                                           '/td[contains(@align,"center")]/nobr/text()')

            n = len(polling_stations)
            polling_stations = np.array(polling_stations).reshape((n, 1))
            table = np.array(table).reshape(20, n)

            names = np.concatenate((np.full((n, 1), region), np.full((n, 1), district),
                                    polling_stations, table.T), axis=1)

            df = df.append(pd.DataFrame(names), ignore_index=True)

    print("parsing is done!")

    df.columns = columns
    df.to_csv('data/election_data.csv', sep='\t', encoding='utf-8')

    print("data is saved")