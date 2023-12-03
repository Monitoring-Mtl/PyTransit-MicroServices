[![CC BY 4.0][cc-by-shield]][cc-by]

# PyTransit-MicroServices
Services Python pour récupérer et analyser les données


## STM_Fetch_GTFS_TripUpdates

Service Python qui permet de récupérer les données GTFS Live pour l'estimation des horaires de la STM et d'enregistrer le fichier JSON de réponse dans un format GZIP (afin de diminuer l'espace requis) dans un bucket S3 dans un répertoire correspondant à la date de la journée.


## STM_Fetch_GTFS_VehiclePositions 

Service Python qui permet de récupérer les données GTFS Live pour les positions des véhicules de la STM et d'enregistrer le fichier JSON de réponse dans un format GZIP (afin de diminuer l'espace requis) dans un bucket S3 dans un répertoire correspondant à la date de la journée.


## STM_Fetch_Update_Static_files

Service Python qui permet de récupérer les fichiers static GTFS de la STM et de les déposer dans un bucket S3 selon une structure de répertoire. La fonction 
valide si les fichiers présents sont les derniers à jour, sinon elle récupère les nouveaux fichiers et les met à jour.

## STM_Filter_Daily_GTFS_Static_files

Service Python qui permet de créer la liste de service_id, de trip_id et de stop_times valident pour la journée à partir des fichier static. Et de les déposer dans un nouveau 
bucket S3. 


## STM_Analyse_Daily_Stops_Data

Service Python qui permet de d'analyser le Delta des autobus par rapport au temps prévu d'arrivé aux arrêts. Il récupère également l'information concernant le niveau d'occupation 
des autobus.

## STM_Merge_Daily_GTFS_VehiclePositions

Service Python qui permet de concaténer l'ensemble des fichiers GTFS VehiclePosition acquis dans une journée. Le processus pour concaténer durant la journée prend trops de temps
lors de l'exécution de la Lambda du service "STM_Fetch_GTFS_VehiclePositions", c'est la raison pour laquelle nous effectuons la fusionner de tout les fichiers dans un seul fichier 
".parquet" qui sera utiliser lors des analyses ou pour la récupération d'informations sur une journée.

# BIXI

## BIXI_Fetch_GTFS_Station_Status

Service Python qui permet de récupérer les données GBFS pour les Status de toutes les stations BIXI et d'enregistrer le fichier JSON de réponse dans un format GZIP, dans un
bucket S3, dans un répertoire correspondant à la date de la journée.


## LICENSE

This work is licensed under a
[Creative Commons Attribution 4.0 International License][cc-by].

[cc-by]: http://creativecommons.org/licenses/by/4.0/
[cc-by-image]: https://i.creativecommons.org/l/by/4.0/88x31.png
[cc-by-shield]: https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg
