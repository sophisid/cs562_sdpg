#!/bin/bash

ROOT_DIR="/mnt/fast/sophisid"
PROJECT_DIR="/mnt/fast/sophisid/cs562_sdpg"
DATASETS_DIR="$PROJECT_DIR/noisy_datasets/LDBC"
SCRIPT_DIR="$PROJECT_DIR/scripts"
SCHEMA_DISCOVERY_DIR="$PROJECT_DIR/schemadiscovery"
NEO4J_TAR="neo4j-community.tar.gz"
NEO4J_VERSION="neo4j-community-4.4.0"
NEO4J_DIR="$NEO4J_VERSION"
OUTPUT_BASE_DIR="$PROJECT_DIR/output"
NEO4J_PORT=7687  # Specify the Neo4j port to kill the process dynamically

mkdir -p "$OUTPUT_BASE_DIR"

datasets=($(ls "$DATASETS_DIR" | grep "corrupted"))

# Βρόχος μέσω κάθε dataset
for dataset in "${datasets[@]}"
do
    echo "==============================="
    echo "Επεξεργασία Dataset: $dataset"
    echo "==============================="

    # Διαδρομή του τρέχοντος dataset
    current_dataset_dir="$DATASETS_DIR/$dataset"

    # Βήμα 1: Διαγραφή της τρέχουσας βάσης δεδομένων Neo4j
    echo "Διαγραφή του $NEO4J_VERSION directory..."
    rm -rf "$NEO4J_DIR"

    # Βήμα 2: Επαναφόρτωση του Neo4j από το tar.gz
    echo "Εξαγωγή του Neo4j από $NEO4J_TAR..."
    tar -xzvf "$NEO4J_TAR" 
 
    # Βήμα 3: Εισαγωγή των CSV αρχείων στο Neo4j
    echo "Εισαγωγή δεδομένων στο Neo4j..."
    "$NEO4J_DIR/bin/neo4j-admin" import --database=neo4j --delimiter='|' \
        --nodes=Comment="$current_dataset_dir/comment_0_0_corrupted.csv" \
        --nodes=Forum="$current_dataset_dir/forum_0_0_corrupted.csv" \
        --nodes=Person="$current_dataset_dir/person_0_0_corrupted.csv" \
        --nodes=Post="$current_dataset_dir/post_0_0_corrupted.csv" \
        --nodes=Place="$current_dataset_dir/place_0_0_corrupted.csv" \
        --nodes=Organisation="$current_dataset_dir/organisation_0_0_corrupted.csv" \
        --nodes=TagClass="$current_dataset_dir/tagclass_0_0_corrupted.csv" \
        --nodes=Tag="$current_dataset_dir/tag_0_0_corrupted.csv" \
        --relationships=HAS_CREATOR="$current_dataset_dir/comment_hasCreator_person_0_0_corrupted.csv" \
        --relationships=HAS_TAG="$current_dataset_dir/comment_hasTag_tag_0_0_corrupted.csv" \
        --relationships=IS_LOCATED_IN="$current_dataset_dir/comment_isLocatedIn_place_0_0_corrupted.csv" \
        --relationships=REPLY_OF="$current_dataset_dir/comment_replyOf_comment_0_0_corrupted.csv" \
        --relationships=REPLY_OF="$current_dataset_dir/comment_replyOf_post_0_0_corrupted.csv" \
        --relationships=CONTAINER_OF="$current_dataset_dir/forum_containerOf_post_0_0_corrupted.csv" \
        --relationships=HAS_MEMBER="$current_dataset_dir/forum_hasMember_person_0_0_corrupted.csv" \
        --relationships=HAS_MODERATOR="$current_dataset_dir/forum_hasModerator_person_0_0_corrupted.csv" \
        --relationships=HAS_TAG="$current_dataset_dir/forum_hasTag_tag_0_0_corrupted.csv" \
        --relationships=HAS_INTEREST="$current_dataset_dir/person_hasInterest_tag_0_0_corrupted.csv" \
        --relationships=IS_LOCATED_IN="$current_dataset_dir/person_isLocatedIn_place_0_0_corrupted.csv" \
        --relationships=KNOWS="$current_dataset_dir/person_knows_person_0_0_corrupted.csv" \
        --relationships=LIKES="$current_dataset_dir/person_likes_comment_0_0_corrupted.csv" \
        --relationships=LIKES="$current_dataset_dir/person_likes_post_0_0_corrupted.csv" \
        --relationships=STUDIES_AT="$current_dataset_dir/person_studyAt_organisation_0_0_corrupted.csv" \
        --relationships=WORKS_AT="$current_dataset_dir/person_workAt_organisation_0_0_corrupted.csv" \
        --relationships=HAS_CREATOR="$current_dataset_dir/post_hasCreator_person_0_0_corrupted.csv" \
        --relationships=HAS_TAG="$current_dataset_dir/post_hasTag_tag_0_0_corrupted.csv" \
        --relationships=IS_LOCATED_IN="$current_dataset_dir/post_isLocatedIn_place_0_0_corrupted.csv" \
        --relationships=IS_LOCATED_IN="$current_dataset_dir/organisation_isLocatedIn_place_0_0_corrupted.csv" \
        --relationships=IS_PART_OF="$current_dataset_dir/place_isPartOf_place_0_0_corrupted.csv" \
        --relationships=HAS_TYPE="$current_dataset_dir/tag_hasType_tagclass_0_0_corrupted.csv" \
        --relationships=IS_SUBCLASS_OF="$current_dataset_dir/tagclass_isSubclassOf_tagclass_0_0_corrupted.csv"

    # Βήμα 4: Εκκίνηση του Neo4j
    echo "Εκκίνηση του Neo4j..."
    "$NEO4J_DIR/bin/neo4j" start

    # Περιμένετε μέχρι να ολοκληρωθεί η εκκίνηση του Neo4j
    echo "Περιμένει για την εκκίνηση του Neo4j..."
    sleep 100  # Αυξήστε τον χρόνο αν χρειάζεται

    # Βήμα 5: Εκτέλεση του Scala προγράμματός σου
    echo "Εκτέλεση του Scala προγράμματός σου..."
    cd "$SCHEMA_DISCOVERY_DIR" || { echo "Δεν μπόρεσα να μεταβώ στο $SCHEMA_DISCOVERY_DIR"; exit 1; }
    sbt run > "$OUTPUT_BASE_DIR/output_LDBC_${dataset#corrupted}.txt"
    cd "$ROOT_DIR" || { echo "Δεν μπόρεσα να επιστρέψω στο $ROOT_DIR"; exit 1; }

    # Βήμα 6: Διακοπή του Neo4j
    echo "Διακοπή του Neo4j..."
    "$NEO4J_DIR/bin/neo4j" stop
    sleep 60  # Περιμένει 60 δευτερόλεπτα για να σιγουρευτεί ότι ο Neo4j σταμάτησε

    # Βήμα 7: Δυναμική διαγραφή διαδικασίας στη θύρα 7687
    echo "Εύρεση και διακοπή οποιασδήποτε διαδικασίας που χρησιμοποιεί τη θύρα $NEO4J_PORT..."
    PID=$(lsof -t -i :$NEO4J_PORT)

    if [ -z "$PID" ]; then
        echo "Δεν υπάρχει καμία διεργασία στη θύρα $NEO4J_PORT."
    else
        echo "Σκοτώνω τη διεργασία με PID: $PID"
        kill -9 $PID
        if [ $? -eq 0 ]; then
            echo "Η διεργασία $PID σταμάτησε επιτυχώς."
        else
            echo "Αποτυχία διακοπής της διεργασίας $PID."
        fi
    fi

    echo "Τέλος επεξεργασίας για το dataset: $dataset"
    echo ""
done

echo "Όλοι οι datasets έχουν επεξεργαστεί."
