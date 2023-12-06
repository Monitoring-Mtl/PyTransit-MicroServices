[![CC BY 4.0][cc-by-shield]][cc-by]

<!-- Improved compatibility of haut link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->



<!-- SHIELDS PROJET -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

<div align="center">

  <a href="">![GitHub contributors](https://img.shields.io/github/contributors/Monitoring-Mtl/PyTransit-MicroServices?color=green)</a>
  <a href="">![GitHub last commit (branch)](https://img.shields.io/github/last-commit/Monitoring-Mtl/PyTransit-MicroServices/master)</a>
  <a href="">![GitHub issues](https://img.shields.io/github/issues/Monitoring-Mtl/PyTransit-MicroServices)</a>
  <a href="">![GitHub top language](https://img.shields.io/github/languages/top/Monitoring-Mtl/PyTransit-MicroServices)</a>

</div>
<!-- [![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url] -->




<!-- LOGO ETS -->
<br />
<div align="center">
  <a href="https://www.etsmtl.ca/">
    <img src="https://www.etsmtl.ca/getmedia/a38cc621-8248-453b-a24e-ff22bd68ada5/Logo_ETS_SansTypo_FR" alt="Logo" width="200" height="200">
  </a>

  <h3 align="center">Monitoring Mtl - PyTransit</h3>

  <p align="center">
    Projet de fin d'etude - Automne 2023 - ETS Montreal
    <br />
  </p>
</div>



<!-- TABLE DES MATIÈRES -->
<details>
  <summary>Table des matières</summary>
  <ol>
    <li>
      <a href="#a-propos-du-projet">A propos du projet</a>
      <ul>
        <li><a href="#construit-avec">Construit avec</a></li>
      </ul>
    </li>
    <li>
      <a href="#pour-débuter">Pour débuter</a>
      <ul>
        <li><a href="#prérequis">Prérequis</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#utilisation">Utilisations</a></li>
      <ul>
        <li><a href="#rapport-de-couverture-de-tests">Rapport de couverture de tests</a></li>
      </ul>
    <li><a href="#plan-de-développement">Plan de développement</a></li>
    <li><a href="#contribuer">Contribuer</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#reconnaissances">Reconnaissances</a></li>
  </ol>
</details>



<!-- A PROPOS DU PROJET -->
## A propos du project
</br>
<div align="center">
  <a href="https://www.etsmtl.ca/">
    <img src="https://github.com/Monitoring-Mtl/Serverless-API/assets/113111772/f4646e57-50f7-4394-a698-2e81f886870e" alt="Logo" width="200" height="200">
  </a>
</div>
</br>

Dans le contexte des villes intelligentes, des systèmes de transport intelligents, et de la mobilité
durable, le développement de solutions logicielles permettant de mieux comprendre l'impact de divers
facteurs (par exemple, les incidents routiers, la construction et la météo) sur la circulation devient un
élément essentiel de la gestion du trafic routier. L’émergence récente des données ouvertes, comme
celles fournies par la Ville de Montréal et la STM, permet maintenant de développer divers types de
solutions/applications logicielles qui contribuent à l’amélioration de la qualité de vie dans les villes.

</br>

Le but de cette application:
* Développer une application infonuagique qui collect des données (en temps réels)
* Analyser les données recueillient
* Construire une interface pour visualiser les données analyser

</br>

  _**Les fonctionnalités mentionnées ci-dessus sont à titre d'exemple pour clarifier l'objectif de chaque
composant (micro-service)_

<p align="right">(<a href="#readme-top">haut</a>)</p>

### Construit Avec

Voici la liste des frameworks et des outils que nous utilisons dans le projet : 

* [![Python][Python]][Python-url]
* [![AWS][AWS]][AWS-url]
* [![Parquet][Parquet][Parquet-url]
* [![Pandas][Pandas]][Pandas-url]
* [![Polars][Polars][Polars-url]
* [![GitHub][GitHub]][GitHub-url]
* [![GitHubActions][GitHubActions]][GitHubActions-url]

<p align="right">(<a href="#readme-top">haut</a>)</p>

# PyTransit-MicroServices

</br>
<div align="center">
  <a href="https://www.etsmtl.ca/">
    <img src="https://github.com/Monitoring-Mtl/PyTransit-MicroServices/blob/main/PyTransit.png?raw=true" alt="Logo" width="200" height="200">
  </a>
</div>
</br>
<div align="center">
  <p>
  Services Python pour récupérer et analyser les données
  </p>
</div>

</br>

<!-- EXPLICATION DES FUNCTIONS -->

## Fonctions - STM

### STM_Fetch_GTFS_TripUpdates

Service Python qui permet de récupérer les données GTFS Live pour l'estimation des horaires de la STM et d'enregistrer le fichier JSON de réponse dans un format GZIP (afin de diminuer l'espace requis) dans un bucket S3 dans un répertoire correspondant à la date de la journée.


### STM_Fetch_GTFS_VehiclePositions 

Service Python qui permet de récupérer les données GTFS Live pour les positions des véhicules de la STM et d'enregistrer le fichier JSON de réponse dans un format GZIP (afin de diminuer l'espace requis) dans un bucket S3 dans un répertoire correspondant à la date de la journée.


### STM_Fetch_Update_Static_files

Service Python qui permet de récupérer les fichiers static GTFS de la STM et de les déposer dans un bucket S3 selon une structure de répertoire. La fonction 
valide si les fichiers présents sont les derniers à jour, sinon elle récupère les nouveaux fichiers et les met à jour.

### STM_Filter_Daily_GTFS_Static_files

Service Python qui permet de créer la liste de service_id, de trip_id et de stop_times valident pour la journée à partir des fichier static. Et de les déposer dans un nouveau 
bucket S3. 


### STM_Analyse_Daily_Stops_Data

Service Python qui permet de d'analyser le Delta des autobus par rapport au temps prévu d'arrivé aux arrêts. Il récupère également l'information concernant le niveau d'occupation 
des autobus.

### STM_Merge_Daily_GTFS_VehiclePositions

Service Python qui permet de concaténer l'ensemble des fichiers GTFS VehiclePosition acquis dans une journée.Le processus pour concaténer au fur et à mesure des "fetch" durant la journée prend trops de temps lors de l'exécution de la Lambda du service "STM_Fetch_GTFS_VehiclePositions", c'est la raison pour laquelle nous effectuons la fusionner de tout les fichiers dans un seul fichier ".parquet" qui sera utiliser lors des analyses ou pour la récupération d'informations sur une journée.

<p align="right">(<a href="#readme-top">haut</a>)</p>

</br>

## Fonctions - BIXI

### BIXI_Fetch_GTFS_Station_Status

Service Python qui permet de récupérer les données GBFS pour les Status de toutes les stations BIXI et d'enregistrer le fichier JSON de réponse dans un format GZIP, dans un
bucket S3, dans un répertoire correspondant à la date de la journée.

<p align="right">(<a href="#readme-top">haut</a>)</p>

</br>

<!-- POUR DÉBUTER -->
## Pour Débuter

### Prérequis

Important d'avoir Python 3.8 ou plus récente d'installer, si non, [cliquez-ici](https://docs.python.org/3.8/)

### Installation

_Vous trouverez ci-bas un exemple de la procédure pour installer le projet localement, ainsi que lancer le projet pour tester que votre environnement de travail est fonctionnel._

1. Obtenir une clé d'API gratuite de la STM a [Stm Portail Développeur](https://portail.developpeurs.stm.info/apihub/?_gl=1*15e9526*_ga*MTUwNTUwMzAzMi4xNjk1MDU5MDA1*_ga_37MDMXFX83*MTY5NjM0NDc3MC4xMi4wLjE2OTYzNDQ3NzAuNjAuMC4w#/login)
2. Cloner le repertoire
   ```sh
   git clone https://github.com/Monitoring-Mtl/PyTransit-MicroServices.git
   ```
3. Install NPM packages
   ```sh
   npm install
   ```
4. Lancer le serverless local
   ```sh
   npm run offline
   ```

<p align="right">(<a href="#readme-top">haut</a>)</p>


<!-- EXAMPLES D'UTILISATION -->
## Utilisation

Pour utiliser les endpoints de l'application visiter [swagger](https://monitoring-mtl.github.io/Swagger-github-pages/) </br>
Pour contribuer ou mettre a jour la documentation Swagger, voir ce [repertoire](https://github.com/Monitoring-Mtl/Swagger-github-pages)

### Rapport de couverture de tests
Pour visualier le rapport de couverture de tests et ses résultats, visiter : [coverage-report](https://monitoring-mtl.github.io/Serverless-API/)

_Pour plus d'informations, référez-vous à la [Documentation](https://github.com/Monitoring-Mtl/Serverless-API/wiki)_
<p align="right">(<a href="#readme-top">haut</a>)</p>



<!-- PLAN -->
## Plan de Développement

- [x] Ajuster et automatiser le Pipeline
- [x] Collection des données

Voir les [issues](https://github.com/Monitoring-Mtl/Serverless-API/issues) pour voir les Features (et issues connuent).

<p align="right">(<a href="#readme-top">haut</a>)</p>



<!-- CONTRIBUER -->
## Contribuer

En esperant que le projet continue de croître grâce a vos contributions. **Merci**

Nous avons décider d'utiliser une structure de Trunk base pour la gestion des branches. Il est donc suggérer de toujours faire une branche a partir de master en suivant la procédure suivante :

1. Cloner le Project (_si ce n'est pas déjà fait_)
2. Creer un branche Feature ou Documentation (`git checkout -b feature/AmazingFeature`)
3. Commit vos Changements (`git commit -m 'ajout d'une AmazingFeature'`)
4. Pousser la Branch (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request et associer votre issue à la PR.

<p align="right">(<a href="#readme-top">haut</a>)</p>

<!-- CONTACT -->
## Contact

- Francis Bordeleau - [@linkedin](https://www.linkedin.com/in/francis-bordeleau-b2aa273/)
- Julien Gascon-Samson - [@linkedin](https://www.linkedin.com/in/julien-gascon-samson-4585b11a/)
- Mohammed Sayagh - [@linkedin](https://www.linkedin.com/in/mohammed-sayagh-24bab978/)

### Étudiants

- Pierre Amar Abdelli - [@linkedin](https://www.linkedin.com/in/pabdelli/)
- Alexandre Bouillon - [@linkedin](https://www.linkedin.com/in/alexandre-bouillon-4b67ba128/)
- Philippe Lamy - [@linkedin](https://www.linkedin.com/in/philippe-lamy-86717a276/)
- Simon St-Pierre - [@linkedin](https://www.linkedin.com/in/simon-st-pierre-9a2a7b19b/)

<p align="right">(<a href="#readme-top">haut</a>)</p>



<!-- RECONNAISSANCES -->
## Reconnaissances

Remerciements à 

  * [Badges](https://github.com/Ileriayo/markdown-badges#markdown-badges)
  * [Readme Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#readme-top">haut</a>)</p>

<!-- A RAJOUTER DANS LE DOCUMENT

Données Ouverte iBUS - App
API Key : l7cb798b78334c48b2b6e4bd9513a221e9

#Decision Relative a GitHub // Pas dans readme

Expliquer pourquoi nous avons choisis le trunk-based development.

#Structure des branches // Faire wiki 

Les branches doivent etre nommber avec le numero de issue generer dans le kanban. Doive etre associer a un pull request documenter. -->


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/othneildrew/Best-README-Template.svg?style=for-the-badge
[contributors-url]: https://github.com/Monitoring-Mtl/Serverless-API/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/othneildrew/Best-README-Template.svg?style=for-the-badge
[forks-url]: https://github.com/othneildrew/Best-README-Template/network/members
[stars-shield]: https://img.shields.io/github/stars/othneildrew/Best-README-Template.svg?style=for-the-badge
[stars-url]: https://github.com/othneildrew/Best-README-Template/stargazers
[issues-shield]: https://img.shields.io/github/issues/othneildrew/Best-README-Template.svg?style=for-the-badge
[issues-url]: https://github.com/othneildrew/Best-README-Template/issues
[license-shield]: https://img.shields.io/github/license/othneildrew/Best-README-Template.svg?style=for-the-badge
[license-url]: https://github.com/othneildrew/Best-README-Template/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/othneildrew
[product-screenshot]: images/screenshot.png
[AWS]: https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white
[AWS-url]: https://aws.amazon.com/
[GitHub]: https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white
[GitHub-url]: https://www.github.com
[GitHubActions]: https://img.shields.io/badge/github%20actions-%232671E5.svg?style=for-the-badge&logo=githubactions&logoColor=white
[GitHubActions-url]: https://github.com/features/actions
[AWS]: https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white
[AWS-url]: https://aws.amazon.com/
[Python]: https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54
[Python-url]: https://docs.python.org/3/
[Pandas]: https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white
[Pandas-url]: https://pandas.pydata.org/
[Polars]: https://img.shields.io/badge/-Polars-CD792C?style=for-the-badge&logo=polars&logoColor=white
[Polars-url]: https://pola.rs/
[Parquet]: https://img.shields.io/badge/-apacheparquet-50ABF1?style=for-the-badge&logo=apacheparquet&logoColor=white
[Parquet-url]: https://parquet.apache.org/


## LICENSE

This work is licensed under a
[Creative Commons Attribution 4.0 International License][cc-by].

[cc-by]: http://creativecommons.org/licenses/by/4.0/
[cc-by-image]: https://i.creativecommons.org/l/by/4.0/88x31.png
[cc-by-shield]: https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg
