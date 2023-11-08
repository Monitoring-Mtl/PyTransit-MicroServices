# PyTransit-MicroServices
Services Python pour récupérer et analyser les données

## STM_Fetch_GTFS_TripUpdates

Service Python qui permet de récupérer les données GTFS Live pour l'estimation des horaires de la STM et d'enregistrer le fichier JSON de réponse dans un format GZIP (afin de diminuer l'espace requis) dans un bucket S3 dans un répertoire correspondant à la date de la journée.


## STM_Fetch_GTFS_VehiclePositions 

Service Python qui permet de récupérer les données GTFS Live pour les positions des véhicules de la STM et d'enregistrer le fichier JSON de réponse dans un format GZIP (afin de diminuer l'espace requis) dans un bucket S3 dans un répertoire correspondant à la date de la journée.


## STM_Fetch_Update_Static_files

Service Python qui permet de récupérer les fichiers static GTFS de la STM et de les déposer dans un bucket S3 selon une structure de répertoire. La fonction 
valide si les fichiers présents sont les derniers à jour, sinon elle récupère les nouveaux fichiers et les met à jour.