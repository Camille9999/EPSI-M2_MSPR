# Plan d'action

## 1. Phase 1 : cadrage et couverture des compétences

Définir le périmètre minimal démontrable : données RTE, pipeline de préparation, comparaison de modèles, API légère, Docker, CI/CD, simulation de déploiement, dossiers Bloc 3 et Bloc 4, soutenances.

## 2. Phase 1 : matrice compétences vers preuves

Lister chaque compétence évaluée des blocs 3 et 4 et lui associer au moins une preuve concrète :
document, schéma, simulation, procédure, tableau de bord, issue GitHub, démonstration technique ou slide.

## 3. Phase 1 : organisation de l’équipe

Répartir le travail ainsi :

- Data/ML : données, features, modèles, métriques.
- Déploiement/MLOps : API, Docker, CI/CD, simulation de déploiement.
- Projet/Docs : cadrage, backlog, KPI, inclusion, communication, soutenances.

## 4. Phase 1 : pilotage dans GitHub Projects

Créer un board simple avec backlog, à faire, en cours, en revue, terminé.
Créer les issues à partir des livrables attendus et non à partir d’idées vagues.

## 5. Phase 2 : acquisition et qualification des données

Identifier un jeu RTE exploitable rapidement, le nettoyer, documenter les hypothèses, choisir les variables minimales utiles :
date, consommation, type de jour, saisonnalité, et température seulement si elle est facile à intégrer sans diluer le temps disponible.

## 6. Phase 2 : architecture cible minimale

Concevoir un flux simple et défendable :
données brutes, préparation, entraînement batch, sauvegarde du modèle, API de prédiction, image Docker, pipeline GitHub, journaux et métriques de base.

## 7. Phase 2 : choix de modèles réalistes

Prioriser des modèles faisables dans le temps :
arbre de décision, random forest, KNN, et un réseau simple si faisable.
Si le RBF pur est trop coûteux, assumer un arbitrage de maintenabilité et l’expliquer.

## 8. Phase 3 : développement du socle data/ML

Construire le pipeline de préparation, le split train/test, l’entraînement multi-modèles, les métriques R2, RMSE, MAPE et le temps d’apprentissage.
Terminer avec une comparaison claire et un modèle retenu.

## 9. Phase 3 : API légère

Exposer au minimum :
un endpoint de santé,
un endpoint de prédiction,
éventuellement un endpoint d’information sur le modèle.
Cette API sert la démonstration, le runbook et les tests.

## 10. Phase 3 : Docker et CI/CD

Conteneuriser l’API et prévoir une CI/CD minimale sur GitHub :
installation des dépendances, vérifications de base, build de l’image, test de démarrage.

## 11. Phase 3 : simulation de déploiement

Préparer des scénarios simples mais crédibles :
montée en charge légère, déploiement d’une nouvelle version, incident simulé, rollback documentaire, vérification post-déploiement.

## 12. Phase 4 : livrables Bloc 3

Produire :

- le dossier de déploiement et de maintenabilité,
- la documentation technique et le runbook,
- la note d’expertise technique,
- le plan d’accompagnement du changement,
- le guide de bonne utilisation de l’IA.

## 13. Phase 5 : livrables Bloc 4

Produire :

- le cadrage projet et cahier des charges,
- l’organisation agile et le backlog,
- les KPI et tableaux de bord,
- la RACI et la cartographie des parties prenantes,
- le plan d’inclusion,
- le plan de communication interculturelle,
- le kit de réunions à distance.

## 14. Phase 6 : soutenances

Préparer deux narratifs différents :

- Bloc 3 : solution, exploitation, simulation, maintenabilité.
- Bloc 4 : gouvernance, agilité, pilotage, inclusion, collaboration.

## 15. Phase 6 : revue finale par compétences

Faire une revue sèche avant rendu :
aucune compétence ne doit rester sans preuve explicite dans le repo ou dans la soutenance.
