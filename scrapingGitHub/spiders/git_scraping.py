import scrapy
import pandas as pd
from neo4j import GraphDatabase 

# Para guardar los datos en la base de datos 
class dbConnector:    

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))        

    def close(self):
        self.driver.close()

    def save_result(  
        self,
        nombre,
        autor,
        readme,
        idioma,
        lenguajes,
        numero_de_vistas,
        fork,
        estrellas,
        paquetes,
        release
    ):
        with self.driver.session() as session:
            resultado = session.write_transaction(
                self.save_result_imp,
                nombre,
                autor,
                readme,
                idioma,
                lenguajes,
                numero_de_vistas,
                fork,
                estrellas,
                paquetes,
                release
            )

            print(resultado)

    @staticmethod
    def save_result_imp(
            tx,
            nombre,
            autor,
            readme,
            idioma,
            lenguajes,
            numero_de_vistas,
            fork,
            estrellas,
            paquetes,
            release
    ):
        result = tx.run(
            "CREATE (n:Resultado {nombre: $nombre, autor: $autor, readme: $readme, idioma: $idioma, lenguajes: $lenguajes, numero_de_vistas: $numero_de_vistas, fork: $fork, estrellas: $estrellas, paquetes: $paquetes, release: $release}) "             
            "RETURN 'Se ha creado el nodo ' + id(n)",
            nombre = nombre,
            autor = autor,
            readme = readme,
            idioma = idioma,
            lenguajes = lenguajes,   
            numero_de_vistas = numero_de_vistas,
            fork = fork,
            estrellas = estrellas,
            paquetes = paquetes,
            release = release
        )

        return result.single()[0]

class GitScraping(scrapy.Spider):
    name = 'scrap'

    start_urls = [
        'https://github.com/TeamTilapia/vscode-tilapia'
    ]

    def parse(self, response):

        lista_nombre = []
        lista_autor = []
        lista_readme = []
        lista_idioma = []
        lista_lenguajes = []
        lista_fecha = []
        lista_numero_vistas = []
        lista_fork = []
        lista_estrellas = []
        lista_paquetes = []
        lista_release = []

        page = response

        # Se inicializa conexión con db
        try:             
            connector = dbConnector(
                'neo4j://localhost:7687/',  # URI
                'neo4j',                    # USER
                'leslielamport'             # PASSWORD
            )
        except:
            print('Existe un error al intentar establecer conexion con la base de datos')

        filename = 'dataFromGitHub.html'
        with open(filename, 'w') as f:

            # Nombre
            nombre = response.xpath('//*[@id="repository-container-header"]/div[1]/div/div/strong/a/text()').get() 
            f.write('Nombre: ' + nombre + ':\n')

            lista_nombre.append(nombre)

            # Autor
            autor = response.xpath('//*[@id="repository-container-header"]/div[1]/div/div/span[1]/a/text()').get()
            f.write('Autor: ' + autor + '\n')

            lista_autor.append(autor)

            # Readme
            readme = response.xpath('string(//*[@id="readme"]/div[2]/article)').get()            
            f.write('Readme: ' + readme.replace('\n', ' ') + '\n')
            
            lista_readme.append(readme.replace('\n', ' '))

            # Idioma
            idioma = response.xpath('string(/html/@lang)').get() 
            f.write('Idioma: ' + idioma + '\n')

            lista_idioma.append(idioma)

            # Lenguajes
            array_lenguajes = response.xpath('//*[@id="repo-content-pjax-container"]/div/div/div[3]/div[2]/div/div[6]/div//text()').extract()            
            string_lenguajes = ''
            for element in array_lenguajes:            
                if element.strip() != '\n' and element.strip() != '' and (element.strip()).count('%') == 0 and element.strip() != 'Languages':
                    string_lenguajes += (element.strip() + '. ' )

            f.write('lenguajes: ' + string_lenguajes + '\n')
            lista_lenguajes.append(string_lenguajes)

            # # Fecha
            # fecha = response.xpath('script(//*[@id="repo-content-pjax-container"]/div/div/div[3]/div[1]/div[2]/div[1]/div/div[2]/div[2]/a[2]/relative-time)').extract()
            # f.write('fecha:' + fecha + '\n')

            # Nùmero de vistas
            numero_de_vistas = response.xpath('string(//*[@id="repo-content-pjax-container"]/div/div/div[3]/div[2]/div/div[1]/div/div[4]/a/strong//text())').get()
            f.write('numero_de_vistas:' + numero_de_vistas + '\n')
            
            lista_numero_vistas.append(numero_de_vistas)

            # Forks
            forks = response.xpath('string(//*[@id="repo-content-pjax-container"]/div/div/div[3]/div[2]/div/div[1]/div/div[5]/a/strong//text())').get()
            f.write('forks:' + forks + '\n')

            lista_fork.append(forks)

            # Estrellas
            estrellas = response.xpath('string(//*[@id="repo-content-pjax-container"]/div/div/div[3]/div[2]/div/div[1]/div/div[3]/a/strong//text())').get()
            f.write('estrellas:' + estrellas + '\n')

            lista_estrellas.append(estrellas)

            # Paquetes
            paquetes = response.xpath('//*[@id="repo-content-pjax-container"]/div/div/div[3]/div[2]/div/div[3]/div/div//text()').get()
            string_paquetes = (paquetes.replace('\n', '')).strip()
            f.write('string_paquetes:' + string_paquetes + '\n')

            lista_paquetes.append(string_paquetes)

            # Releases
            release = response.xpath('//*[@id="repo-content-pjax-container"]/div/div/div[3]/div[2]/div/div[2]/div/div//text()').get()
            f.write('Releases: ' + release + '\n')

            lista_release.append(release)

            csv_data = {
                'Nombre': lista_nombre,
                'Autor': lista_autor,
                'Readme': lista_readme,
                'Idioma': lista_idioma,
                'Lenguajes': lista_lenguajes,
                'Numero de visitas': lista_numero_vistas,
                'Fork': lista_fork,
                'Estrellas': lista_estrellas,
                'Paquetes': lista_paquetes,
                'Release': lista_release
            }

            dataFrame = pd.DataFrame(
                csv_data,
                columns=[
                    'Nombre',
                    'Autor',
                    'Readme',
                    'Idioma',
                    'Lenguajes',
                    'Numero de visitas',
                    'Fork',
                    'Estrellas',
                    'Paquetes',
                    'Release'
                ]
            ) 

            # Crea el archivo pero no toma el número de la fila como un dato para el csv
            dataFrame.to_csv('DataPaginaWeb.csv', index=False) 

            if connector:

                print('Se realiza guardado en base de datos')
                
                # Se guarda resultados en base de datos
                connector.save_result(
                    nombre,
                    autor,
                    readme.replace('\n', ' '),
                    idioma,
                    string_lenguajes,
                    numero_de_vistas,
                    forks,
                    estrellas,
                    string_paquetes,
                    release
                )

                # Se cierra conexion con base de datos
                connector.close() 


            

        