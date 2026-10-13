/************************************************************
 * TEMA 2 – Algoritmi Paraleli și Distribuiți
 * Protocolul CHORD în MPI
 *
 * Studenții trebuie să implementeze:
 *   1. build_finger_table() - construirea finger table CHORD
 *   2. closest_preceding_finger() - rutarea logaritmică
 *   3. handle_lookup_request() - logica de lookup distribuit
 *   4. inițierea lookup-urilor locale
 *   5. service-loop-ul distribuit
 *
 * Ce este deja implementat:
 *   - citirea inputului
 *   - formarea inelului CHORD static
 *   - maparea ID -> rank
 *   - structurile de date CHORD
 *   - primitive pentru succesor, predecesor, ID-uri globale
 *
 * Tema se rezolvă STRICT folosind:
 *   - MPI_Send / MPI_Recv
 *   - mesaje cu tag-urile TAG_LOOKUP_REQ, TAG_LOOKUP_REP și TAG_DONE
 ************************************************************/

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define M 4 // 2^m = 16 ID-uri posibile
#define RING_SIZE 16
#define MAX_NODES 32
#define MAX_PATH 32

/************************************************************
 * Structuri CHORD
 ************************************************************/
typedef struct {
    int start;              // start[i] = (id + 2^i) mod 2^m
    int node;               // finger[i] = succesor(start)
} Finger;

typedef struct {
    int id;                 // ID CHORD al nodului curent
    int successor;          // succesorul în inel
    int predecessor;        // predecesorul în inel
    Finger finger[M];       // finger table-ul (de completat de voi)
} NodeState;

typedef struct {
    int initiator_id;       // cine a inițiat lookup-ul
    int current_id;         // nodul care procesează mesajul
    int key;                // cheia căutată
    int path[MAX_PATH];     // traseul lookup-ului
    int path_len;           // lungimea traseului
} LookupMsg;

/************************************************************
 * Tag-uri mesaje MPI
 ************************************************************/
enum {
    TAG_LOOKUP_REQ = 1,
    TAG_LOOKUP_REP,
    TAG_DONE
};

/************************************************************
 * Variabile globale utile
 ************************************************************/
int world_rank, world_size;
NodeState self;

int all_ids[MAX_NODES];      // ID CHORD pentru fiecare rank
int id_to_rank[RING_SIZE];   // mapare inversă: CHORD ID -> MPI rank
int sorted_ids[MAX_NODES];   // ID-uri sortate ale nodurilor din inel
int num_nodes;

/************************************************************
 * Funcții utile pentru interval circular CHORD
 ************************************************************/
int in_interval(int x, int start, int end) {
    if (start < end)
        return (x > start && x <= end);
    if (start > end)
        return (x > start || x <= end);
    return 1;   // intervalul acoperă tot cercul
}

/************************************************************
 * Construirea mapării rank -> id și id -> rank
 ************************************************************/
void build_id_maps() {
    for (int i = 0; i < RING_SIZE; i++)
        id_to_rank[i] = -1;

    for (int r = 0; r < world_size; r++) {
        int id = all_ids[r];
        if (id >= 0 && id < RING_SIZE)
            id_to_rank[id] = r;
    }
}

int rank_from_id(int id) {
    if (id < 0 || id >= RING_SIZE)
        return -1;
    return id_to_rank[id];
}

/************************************************************
 * Construirea inelului CHORD static
 ************************************************************/
int cmp_int(const void *a, const void *b) {
    return (*(int*)a - *(int*)b);
}

void build_global_ring() {
    num_nodes = world_size;

    for (int i = 0; i < num_nodes; i++)
        sorted_ids[i] = all_ids[i];

    qsort(sorted_ids, num_nodes, sizeof(int), cmp_int);

    for (int i = 0; i < num_nodes; i++) {
        int id = sorted_ids[i];
        int succ = sorted_ids[(i + 1) % num_nodes];
        int pred = sorted_ids[(i - 1 + num_nodes) % num_nodes];

        if (id == self.id) {
            self.successor = succ;
            self.predecessor = pred;
        }
    }
}

/************************************************************
 * Construirea finger table-ului – de completat de voi
 *
 * IMPORTANT:
 *  Tema folosește o variantă STATICĂ a CHORD, iar succesorul unei
 *  poziții "start" se caută EXCLUSIV în lista nodurilor existente,
 *  nu în întreg spațiul 0..RING_SIZE-1 (așa cum ar fi în CHORD real).
 *
 *  find_successor_simple() caută succesorul lui start[i] în sorted_ids[],
 *  care conține DOAR nodurile existente.
 ************************************************************/
int find_successor_simple(int key) {
    for (int i = 0; i < num_nodes; i++)
        if (sorted_ids[i] >= key)
            return sorted_ids[i];
    return sorted_ids[0]; // wrap-around
}

void build_finger_table() {

    /******************************************************
     * TODO 1 – de implementat de voi
     *
     * Atenție:
     *   - succesorul se caută doar în lista sorted_ids
     *   - inelul este static, deci finger table-ul este static
     *   - este parte OBLIGATORIE a temei
     ******************************************************/

    // Pentru fiecare pozitie i din finger table
    for (int i = 0; i < M; i++) {
        // Calculam puterea 2^i
        int p = 1;
        for (int j = 0; j < i; j++) {
            p *= 2;
        }
        // Calculam start[i] si gasim succesorul
        self.finger[i].start = (self.id + p) % RING_SIZE;
        self.finger[i].node = find_successor_simple(self.finger[i].start);
    }
}

/************************************************************
 * closest_preceding_finger – de implementat de voi
 *
 *  Această funcție returnează cel mai "mare" finger care
 *  se află strict în intervalul CHORD (self.id, key).
 *
 * Atenție:
 *    – Intervalul este unul circular și DESCHIS la self.id.
 *    – Finger-ul ales trebuie să ducă lookup-ul mai aproape
 *      de key în mod real.
 *    – Alegerea unui finger care nu produce progres (de ex.
 *      care se întoarce la self.id) poate duce la cicluri
 *      infinite și blocarea rutării.
 *
 *  Dacă niciun finger nu se potrivește, întoarceți succesorul.
 ************************************************************/
int closest_preceding_finger(int key) {

    /******************************************************
     * TODO 2 - de implementat de voi
     ******************************************************/
    
    // Cautam de la cel mai mare finger la cel mai mic
    for (int i = M - 1; i >= 0; i--) {
        // Verificam sa fie un interval circular deschis
        int in_open_interval = 0;
        // fara wrap-around
        if (self.id < key && (self.finger[i].node > self.id && self.finger[i].node < key)) {
            in_open_interval = 1;
        // cu wrap-around circular
        } else if (self.id > key && (self.finger[i].node > self.id || self.finger[i].node < key)) {
            in_open_interval = 1;
        }

        // Returnam primul finger gasit in interval
        if (in_open_interval) {
            return self.finger[i].node;
        }
    }

    return self.successor;   // fallback
}

/************************************************************
 * handle_lookup_request – de implementat de voi
 *
 *   Aceasta este funcția esențială pentru rutare distribuită.
 *   Pașii corecți CHORD (tema simplificată):
 *
 *   1. Adăugați self.id în path.
 *   2. Dacă succesorul nostru este responsabil de key:
 *           - adăugați succesorul în path
 *           - trimiteți TAG_LOOKUP_REP către inițiator
 *   3. Altfel:
 *           - next = closest_preceding_finger(key)
 *           - trimiteți TAG_LOOKUP_REQ către closest_preceding_finger
 *
 ************************************************************/
void handle_lookup_request(LookupMsg *msg) {
    /******************************************************
     * TODO 3 – de implementat de voi
     ******************************************************/

    // Adaugam self.id in path
    msg->path[msg->path_len++] = self.id;
    msg->current_id = self.id;

    // Verificam daca succesorul este responsabil pentru key
    int key = msg->key;
    if (in_interval(key, self.id, self.successor)) {
        // Adaugam succesorul in path
        msg->path[msg->path_len++] = self.successor;
        // Trimitem TAG_LOOKUP_REP inapoi catre initiator
        int initiator_rank = rank_from_id(msg->initiator_id);
        MPI_Send(msg, sizeof(LookupMsg), MPI_BYTE, initiator_rank, TAG_LOOKUP_REP, MPI_COMM_WORLD);
    } else {
        // Fix cum este explicat mai sus
        int next = closest_preceding_finger(key);
        int next_rank = rank_from_id(next);
        MPI_Send(msg, sizeof(LookupMsg), MPI_BYTE, next_rank, TAG_LOOKUP_REQ, MPI_COMM_WORLD);
    }
}

int main(int argc, char **argv) {

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    /******************************************************
     * Citire input
     ******************************************************/
    char fname[32];
    sprintf(fname, "in%d.txt", world_rank);
    FILE *f = fopen(fname, "r");
    if (!f) {
        fprintf(stderr, "Rank %d: cannot open %s\n", world_rank, fname);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int nr_lookups;
    fscanf(f, "%d", &self.id);
    fscanf(f, "%d", &nr_lookups);

    int *lookups = malloc(nr_lookups * sizeof(int));
    for (int i = 0; i < nr_lookups; i++)
        fscanf(f, "%d", &lookups[i]);
    fclose(f);

    /******************************************************
     * Distribuirea ID-urilor tuturor nodurilor
     ******************************************************/
    MPI_Allgather(&self.id, 1, MPI_INT,
                  all_ids, 1, MPI_INT,
                  MPI_COMM_WORLD);

    build_id_maps();
    build_global_ring();
    build_finger_table();

    MPI_Barrier(MPI_COMM_WORLD);

    /************************************************************
     * TODO 4 - inițierea lookup-urilor locale
     *
     *    Pentru fiecare valoare Key citită din input:
     *       - construiți LookupMsg
     *       - trimiteți un mesaj de tip TAG_LOOKUP_REQ către propriul rank
     ************************************************************/

    // Alocam memorie pentru rezultate
    LookupMsg *results = malloc(nr_lookups * sizeof(LookupMsg));
    int *result_received = calloc(nr_lookups, sizeof(int));
    int results_count = 0;

    // Salvam cheile originale pentru a pastra ordinea
    int *lookup_original_keys = malloc(nr_lookups * sizeof(int));
    for (int i = 0; i < nr_lookups; i++) {
        lookup_original_keys[i] = lookups[i];
    }

    // Cream si trimitem mesajele de lookup
    for (int i = 0; i < nr_lookups; i++) {
        // Cream mesajul
        LookupMsg msg;
        msg.initiator_id = self.id;
        msg.current_id = self.id;
        msg.key = lookup_original_keys[i];
        msg.path_len = 0;
        // Trimitem mesajul catre propriul rank
        MPI_Send(&msg, sizeof(LookupMsg), MPI_BYTE, world_rank, TAG_LOOKUP_REQ, MPI_COMM_WORLD);
    }

    free(lookups);

    /************************************************************
     * TODO 5 - service-loop distribuit
     *
     *    Scop:
     *       - primiți mesaje cu MPI_Recv()
     *       - procesați TAG_LOOKUP_REQ → handle_lookup_request()
     *       - procesați TAG_LOOKUP_REP → afișare + countdown local
     *       - trimiteți TAG_DONE tuturor când ați terminat
     *       - opriți loop-ul doar când primiți DONE de la toate nodurile
     ************************************************************/

    // Initializam variabile pentru service-loop
    int d_sent = 0;
    int d_received = 0;
    int all_done = 0;
    // Daca nu avem lookups, suntem deja gata
    if (nr_lookups == 0) {
        all_done = 1;
    }

    // Loop principal
    // Continuam sa primim mesaje pana cand am primit DONE de la toate nodurile
    // Sau am terminat toate look-upurile
    while (d_received < world_size - 1 || results_count < nr_lookups) {
        MPI_Status status;
        LookupMsg msg;

        // Primim mesajul MPI
        MPI_Recv(&msg, sizeof(LookupMsg), MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        // Procesam mesajul in functie de tag
        // TAG_LOOKUP_REQ ce inseamna ca cineva cere un lookup
        if (status.MPI_TAG == TAG_LOOKUP_REQ) {
            // Apelam functia de procesare a request-ului
            handle_lookup_request(&msg);
        // TAG_LOOKUP_REP ce inseamna ca cineva ne trimite rezultatul unui lookup
        } else if (status.MPI_TAG == TAG_LOOKUP_REP) {
            // Cautam indexul bazat pe cheia originala
            for (int i = 0; i < nr_lookups; i++) {
                if (!result_received[i] && lookup_original_keys[i] == msg.key) {
                    // Daca l-am gasit il salvam in rezultate
                    results[i] = msg;
                    result_received[i] = 1;
                    results_count++;
                    break;
                }
            }
            // Verificam daca am terminat toate look-upurile
            if (results_count == nr_lookups) {
                all_done = 1;
            }
        // TAG_DONE ce inseamna ca un alt nod si-a terminat look-upurile
        } else if (status.MPI_TAG == TAG_DONE) {
            // Incrementam contorul de DONE primite
            d_received++;
        }
        // Trimitem DONE catre toate nodurile daca am terminat toate look-upurile
        if (all_done && !d_sent) {
            d_sent = 1;
            for (int r = 0; r < world_size; r++) {
                // Avem grija sa nu ne trimitem noua insine mesajul DONE
                if (r != world_rank) {
                    MPI_Send(&msg, sizeof(LookupMsg), MPI_BYTE, r, TAG_DONE, MPI_COMM_WORLD);
                }
            }
        }
    }
    
    // Afisam rezultatele in initierii look-upurilor
    for (int i = 0; i < nr_lookups; i++) {
        LookupMsg *res = &results[i];
        
        printf("Lookup %d:", res->key);
        for (int j = 0; j < res->path_len; j++) {
            if (j == 0)
                printf(" %d", res->path[j]);
            else
                printf(" -> %d", res->path[j]);
        }
        printf("\n");
    }

    // Eliberam memoria alocata
    free(results);
    free(result_received);
    free(lookup_original_keys);

    MPI_Finalize();
    return 0;
}
