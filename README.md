# Sim-Twin-F1

Pipeline de télémétrie F1 en flux continu : de vraies données de course (API OpenF1) sont rejouées vers Apache Kafka, stockées dans TimescaleDB, puis supervisées avec Prometheus et Grafana.

```
OpenF1 API ──► producer (Python) ──► Kafka ──► sink (Python) ──► TimescaleDB ──► Grafana
                     │                                │
                     └──────── métriques ────────► Prometheus
```

## Fonctionnement

Le producer lit une session de course sur [OpenF1](https://openf1.org) et avance dans le temps à la vitesse réelle (réglable). Chaque mesure est horodatée à l'instant d'envoi : les tableaux de bord voient un flux en direct, même pour une course terminée. Quand la session se termine, elle redémarre au début.

### Topics Kafka

| Topic | Contenu | Clé | Partitions | Particularité |
|---|---|---|---|---|
| `telemetry.raw` | vitesse, accélérateur, frein, rapport, régime, usure pneus | pilote | 6 | conservé 1 jour |
| `telemetry.latest` | mêmes mesures | pilote | 3 | compacté : dernier état de chaque pilote |
| `race.laps` | un message par tour terminé | pilote | 3 | |
| `race.events` | drapeaux, safety car, pénalités | `race` | 1 | ordre global |
| `telemetry.dlq` | messages invalides rejetés par le sink | pilote | 1 | conservé 7 jours |
| `telemetry.kpi`, `telemetry.predictions` | réservés aux étapes suivantes | | 3 | pas encore utilisés |

### Services

- **producer** (`producer/`) : lit OpenF1 et publie sur Kafka. Producer idempotent, `acks=all`, compression lz4, en-têtes `source` et `session_key`. Métriques sur le port 8000.
- **sink** (`sink/`) : consommateur du groupe `f1-sink`. Écrit par lots dans TimescaleDB (`telemetry`, `lap`, `race_event`). Les offsets sont validés à la main après l'écriture en base (au moins une fois). Un message invalide est copié dans `telemetry.dlq` avec l'erreur en en-tête. Métriques sur le port 8003.
- **kafka**, **timescaledb**, **prometheus**, **grafana** : infrastructure, définie dans `docker-compose.yml`.

## Démarrage

Prérequis : Docker et Docker Compose.

```bash
docker compose up -d --build
docker compose logs -f producer sink
```

Vérifier que les données arrivent :

```bash
docker exec f1-timescaledb psql -U f1 -c "select driver, count(*) from telemetry group by 1"
docker exec f1-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic telemetry.raw --max-messages 5
```

| Service | Adresse |
|---|---|
| Grafana | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090 |
| Métriques producer | http://localhost:8000/metrics |
| Métriques sink | http://localhost:8003/metrics |
| Kafka (hôte) | localhost:9092 |
| TimescaleDB | localhost:5432 (f1 / f1pass, base `f1`) |

## Configuration du producer

Variables d'environnement, à définir dans `docker-compose.yml` :

| Variable | Défaut | Rôle |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | adresse des brokers (`kafka:19092` dans Docker) |
| `SESSION_KEY` | `latest` | session OpenF1 à rejouer |
| `DRIVERS` | tous | filtre par acronymes, ex. `VER,NOR` |
| `SPEED` | `1` | vitesse du rejeu (`5` = cinq fois plus vite) |
| `START_MIN` | `0` | minutes après le début de la session |
| `WINDOW_S` | `5` | secondes de données chargées par appel API |
| `LOOP` | `1` | `1` pour recommencer à la fin de la session |
| `METRICS_PORT` | `8000` | port Prometheus |

## Limites connues

- OpenF1 ne fournit pas la température des pneus : `tyre_temp` vaut `null`.
- L'usure des pneus est une estimation (2 % par tour, calculée depuis les relais et les tours), pas une mesure.
- Les tours et événements de course sont chargés une fois au démarrage : ils conviennent au rejeu de sessions passées.
- Suivre une session réellement en cours demande en général un compte OpenF1 payant.
- Le sink garantit « au moins une fois » : un doublon est possible après une panne.
- Le calcul des KPI par fenêtre (`kpi_window`) et les prédictions ne sont pas encore implémentés.

## Structure

```
producer/     producer.py, openf1.py, requirements.txt, Dockerfile
sink/         sink.py, requirements.txt, Dockerfile
sql/          init.sql (tables TimescaleDB)
prometheus/   prometheus.yml
grafana/      provisioning des sources de données
docker-compose.yml
```
