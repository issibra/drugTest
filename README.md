**DrugTest** 

DrugTest implements a date pipeline that:

1) load data from different file (csv, json)
2) Implement data transformation logic and many aggregation
3) Collect all data and produce a json file that holds all the inforamtions

**Possible enhancements**


Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ?

- lecture des données depuis les fichiers csv et json.
- L'ordre des transformations 
- Les jointures 

Pourriez-vous décrire les modifications qu’il faudrait apporter, s’il y en a, pour prendre en considération de telles volumétries ?

1) Lecture des données depuis les fichiers csv et json: 
               - opter pour des tables exéterieures hives se basant sur des fichiers excel (comme input) 
               - Lecture des ficheirs en utilisant la parallelize ou partitions
2) L'ordre des transformations: 
                - opter pour les filtrations des lignes qui sont nécessaire pour le traitement au début
                - Utilisation des cache pour les df utilsié plus que deux fois.
                - Utilisation des dropDuplicate ou distincts pour élminer les lignes dupliqués.
                
3) Les jointures:
                - Opter pour les broadcast des variables ou le braodcast de la jointure pour les fichiers plus petits 
                  lors des jointures entre gros fichiers et petit fichier(celà évitera des opérations de shuffle supplémentaires)

Ainsi il faut veiller sur la qualité de code qu'il soit lisible pour la maintenance et les évolutions pour le futur, une bonne documentation pourra être effectué sur des pages wiki.

Et Finalement il faut surveiller toujours le déroulement des jobs Spark (sur Yarn ou Spark webUi) pour pouvoir détecter et 
suivre la consommation des resrouces et détecter si il y a des stages qui consomme de la mémoire ou qui contient en sortie des volumes très importants.
