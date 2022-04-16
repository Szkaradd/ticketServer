/* Zadanie zaliczeniowe 1. Sieci komputerowe. */
/* Autor: Mikołaj Szkaradek */

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <cstdint>
#include <cctype>
#include <sys/stat.h>
#include <ctime>
#include <endian.h>
#include <vector>
#include <fstream>
#include <cstdlib>
#include <iostream>
#include <cassert>
#include <unordered_map>
#include <deque>

#define MAX_UDP_BUFF_SIZE 65507
#define DECIMAL_BASE 10
#define COOKIE_SIZE 48
#define CHARS_TO_DRAW_COOKIE 94
#define FIRST_ACCEPTED_CHAR 33
#define TICKET_SIZE 7
#define MAX_TICKET_COUNT 9357

#define DEFAULT_PORT_NUMBER 2022
#define DEFAULT_TIMEOUT 5
#define MAX_TIMEOUT 86400
#define MIN_TIMEOUT 1

#define GET_EVENTS 1
#define EVENTS 2
#define GET_RESERVATION 3
#define RESERVATION 4
#define GET_TICKETS 5
#define TICKETS 6
#define DONT_ANSWER 99
#define BAD_REQUEST 255

#define FIRST_RESERVATION_ID 1e6
#define FIRST_TICKET "0000000"
#define LAST_DIGIT_IN_BASE_36 '9'
#define FIRST_DIGIT_IN_BASE_36 '0'
#define LAST_NON_DIGIT_IN_BASE_36 'Z'
#define FIRST_NON_DIGIT_IN_BASE_36 'A'
#define FILE_FLAG "-f"
#define PORT_FLAG "-p"
#define TIMEOUT_FLAG "-t"

// Evaluate `x`: if false, print an error message_info and exit with an error.
#define ENSURE(x)                                                         \
    do {                                                                  \
        bool result = (x);                                                \
        if (!result) {                                                    \
            fprintf(stderr, "Error: %s was false in %s at %s:%d\n",       \
                #x, __func__, __FILE__, __LINE__);                        \
            exit(EXIT_FAILURE);                                           \
        }                                                                 \
    } while (0)

// Check if errno is non-zero, and if so, print an error message_info and exit with an error.
#define PRINT_ERRNO()                                                  \
    do {                                                               \
        if (errno != 0) {                                              \
            fprintf(stderr, "Error: errno %d in %s at %s:%d\n%s\n",    \
              errno, __func__, __FILE__, __LINE__, strerror(errno));   \
            exit(EXIT_FAILURE);                                        \
        }                                                              \
    } while (0)

// Set `errno` to 0 and evaluate `x`. If `errno` changed, describe it and exit.
#define CHECK_ERRNO(x)                                                             \
    do {                                                                           \
        errno = 0;                                                                 \
        (void) (x);                                                                \
        PRINT_ERRNO();                                                             \
    } while (0)

using ticket_t = std::string;

// Shared buffer for sending messages with udp socket.
char shared_buffer[MAX_UDP_BUFF_SIZE];

struct reservation_info;

// Unordered map to store information about reservations
// that haven't been sent to client yet.
std::unordered_map<uint32_t, reservation_info*> active_reservations;

// Unordered map to store information about completed reservations
std::unordered_map<uint32_t, reservation_info*> completed_reservations;

// Function checks whether given path exists.
bool path_exists(const char *path) {
    struct stat st_buf{};
    return (stat(path, &st_buf) == 0);
}

// Function checks if given existing path is a file.
bool is_file(const char *path) {
    struct stat st_buf{};
    stat(path, &st_buf);
    return (S_ISREG(st_buf.st_mode));
}

// Generates random cookie in cookie[] array.
void generate_cookie(char cookie[]) {
    for (size_t i = 0; i < COOKIE_SIZE; i++) {
        cookie[i] = rand() % CHARS_TO_DRAW_COOKIE + FIRST_ACCEPTED_CHAR;
    }
}

// This function increments current ticket value.
// It treats ticket as a number in system with base equal to 36.
void increment_ticket(ticket_t& ticket) {
    int curr_pos = TICKET_SIZE - 1;
    while (curr_pos >= 0) {
        if (ticket.at(curr_pos) == LAST_DIGIT_IN_BASE_36) {
            ticket.at(curr_pos) = FIRST_NON_DIGIT_IN_BASE_36;
            break;
        }
        else if (ticket.at(curr_pos) == LAST_NON_DIGIT_IN_BASE_36) {
            ticket.at(curr_pos) = FIRST_DIGIT_IN_BASE_36;
            curr_pos--;
            if (curr_pos < 0) {
                fprintf(stderr, "No more tickets available.\n");
            }
        }
        else {
            ticket.at(curr_pos)++;
            break;
        }
    }
}

// Struct containing information about one reservation and tickets assigned to it.
struct reservation_info {
    uint32_t reservation_id;
    uint32_t event_id;
    uint16_t ticket_count;
    char cookie[COOKIE_SIZE]{};
    uint64_t expiration_time;
    std::vector<ticket_t> tickets;

    explicit reservation_info(uint32_t reservation_id, uint32_t event_id,
                     uint16_t ticket_count, uint32_t timeout) :
                     reservation_id(reservation_id), event_id(event_id),
                     ticket_count(ticket_count){
        generate_cookie(cookie);
        expiration_time = time(nullptr) + timeout;
    }
    ~reservation_info() = default;
};

// Struct containing information about one event.
struct event_info {
    uint32_t event_id;
    uint16_t ticket_count;
    uint8_t description_length;
    std::string description;
    event_info(uint8_t description_length, uint16_t ticket_count,
               uint32_t event_id, const char *description) : event_id(event_id),
                     ticket_count(ticket_count), description_length(description_length),
                     description(description){};
    ~event_info() = default;
};

// Struct containing information about all events.
struct event_list_t {
    std::vector<event_info*> events;
    size_t events_count;
    event_list_t() : events_count(0){};

    ~event_list_t() {
        for (size_t i = 0; i < events_count; i++) {
            delete(events[i]);
        }
    }
};

// Struct to store information about parameters given to server while launching.
struct parametersGiven {
    bool file_given;
    std::string file;
    uint16_t port;
    uint32_t timeout;

    parametersGiven() : file_given(false), port(DEFAULT_PORT_NUMBER), timeout(DEFAULT_TIMEOUT){};

    ~parametersGiven() = default;
};

// Function generates event list from file at given path.
event_list_t *event_list_from_file(const std::string& path) {
    auto event_list = new event_list_t();

    std::ifstream file(path);
    if (!file.is_open()) {
        fprintf(stderr, "Could not open file.\n");
        exit(EXIT_FAILURE);
    }
    uint16_t ticket_count;
    std::string description;
    uint32_t event_count = 0;

    while(getline(file, description)) {
        file >> ticket_count;
        uint8_t description_length = description.length();
        auto event = new event_info(description_length, ticket_count, event_count, description.c_str());
        event_list->events.push_back(event);
        event_count++;
        file.ignore();
    }
    event_list->events_count = event_count;

    file.close();
    return event_list;
}

// Struct containing information about message received from client.
struct message_info {
    uint8_t message_id;
    uint16_t ticket_count;
    uint32_t event_id;
    uint32_t reservation_id;
    uint32_t error_id;
    char cookie[COOKIE_SIZE + 1];
};

// Function checks if port passed in *number is valid.
bool is_valid_port_number(const char* number) {
    if (number == nullptr) return false;
    if (number[0] == '0' || !isdigit(number[0]))
        return false;
    for (int i = 1; number[i] != 0; i++) {
        if (!isdigit(number[i]))
            return false;
    }

    char *end;
    errno = 0;
    unsigned long port = strtoul(number, &end, DECIMAL_BASE);
    if (errno != 0 || port > UINT16_MAX)
        return false;
    return true;
}

// Function checks if timeout passed in *number is valid.
bool is_valid_timeout(const char *number) {
    if (number == nullptr) return false;
    if (number[0] == '0' || !isdigit(number[0]))
        return false;
    for (int i = 1; number[i] != 0; i++)
    {
        if (!isdigit(number[i]))
            return false;
    }

    char *end;
    errno = 0;
    unsigned long port = strtoul(number, &end, DECIMAL_BASE);
    if (errno != 0 || port > MAX_TIMEOUT || port < MIN_TIMEOUT)
        return false;
    return true;
}

uint16_t read_port(char *string) {
    unsigned long port = strtoul(string, nullptr, DECIMAL_BASE);
    return (uint16_t) port;
}

uint32_t read_timeout(char *string) {
    unsigned long timeout = strtoul(string, nullptr, DECIMAL_BASE);
    return (uint32_t) timeout;
}

void exit_with_usage_error(const char *error_msg, const char *filename) {
    fprintf(stderr, "%s", error_msg);
    fprintf(stderr, "usage: %s -f <file> optional: -p <port> -t <timeout>\n", filename);
    exit(EXIT_FAILURE);
}

// Function checks the correctness of server parameters.
// Return struct containing information about server parameters.
parametersGiven *check_server_parameters(int argc, char *argv[]) {
    auto server_parameters = new parametersGiven();
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], FILE_FLAG) == 0) {
            if (!path_exists(argv[i + 1])) {
                exit_with_usage_error("Path does not exist.\n", argv[0]);
            }
            if (!is_file(argv[i + 1])) {
                exit_with_usage_error("Specified path does not represent a file.\n", argv[0]);
            }
            server_parameters->file_given = true;

            server_parameters->file = argv[i + 1];
            i++;
        }
        else if (strcmp(argv[i], PORT_FLAG) == 0) {
            if (!is_valid_port_number(argv[i + 1])) {
                exit_with_usage_error("Invalid port number. Should be between [1, 65535].\n", argv[0]);
            }

            server_parameters->port = read_port(argv[i + 1]);
            i++;
        }
        else if (strcmp(argv[i], TIMEOUT_FLAG) == 0) {
            if (!is_valid_timeout(argv[i + 1])) {
                exit_with_usage_error("Invalid timeout number. Should be between [1, 86400].\n", argv[0]);
            }

            server_parameters->timeout = read_timeout(argv[i + 1]);
            i++;
        }
        else {
            fprintf(stderr, "Parameter %s is not expected.\n", argv[i]);
            fprintf(stderr, "usage: %s -f <file> (optional: -p <port> -t <timeout>)\n", argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    if (!server_parameters->file_given) {
        exit_with_usage_error("No file was given.\n", argv[0]);
    }

    return server_parameters;
}

// Function from UDP scenario from labs.
int bind_socket(uint16_t port) {
    int socket_fd = socket(AF_INET, SOCK_DGRAM, 0); // Creating IPv4 UDP socket.
    ENSURE(socket_fd > 0);

    struct sockaddr_in server_address{};
    server_address.sin_family = AF_INET; // IPv4
    server_address.sin_addr.s_addr = htonl(INADDR_ANY); // Listening on all interfaces.
    server_address.sin_port = htons(port);

    // Bind the socket to a concrete address.
    CHECK_ERRNO(bind(socket_fd, (struct sockaddr *) &server_address,
                     (socklen_t) sizeof(server_address)));

    return socket_fd;
}

// Adds new reservation to active reservations.
void create_reservation(uint32_t reservation_id, uint32_t event_id, uint16_t ticket_count,
                       uint32_t timeout, event_list_t *event_list) {

    event_list->events[event_id]->ticket_count -= ticket_count;
    auto rInfo = new reservation_info(reservation_id, event_id, ticket_count, timeout);
    active_reservations[reservation_id] = rInfo;
}

// Function checks whether reservation can be performed.
void check_reservation_and_fill_msg_info(event_list_t *event_list, const char *buffer, message_info *message) {
    uint32_t event_id = be32toh(*((uint32_t *) buffer));
    message->event_id = event_id;
    message->error_id = event_id;

    buffer += sizeof(uint32_t);
    uint32_t ticket_count = be16toh(*((uint16_t *) buffer));
    message->ticket_count = ticket_count;

    if (event_id >= event_list->events_count) {
        message->message_id = BAD_REQUEST;
        return;
    }

    if (ticket_count == 0 || event_list->events[event_id]->ticket_count < ticket_count || ticket_count > MAX_TICKET_COUNT) {
        message->message_id = BAD_REQUEST;
    }
}

// Function checks if request for tickets can be realized.
void check_tickets_and_fill_msg_info(const char *buffer, message_info *message) {
    uint32_t reservation_id = be32toh(*((uint32_t *) buffer));
    message->reservation_id = reservation_id;
    message->error_id = reservation_id;
    buffer += sizeof(uint32_t);
    strncpy(message->cookie, buffer, COOKIE_SIZE);

    // If there is no active reservation look for it in completed reservations.
    if (active_reservations.find(reservation_id) == active_reservations.end()) {
        if (completed_reservations.find(reservation_id) == active_reservations.end()) {
            message->message_id = BAD_REQUEST;
        }
        else {
            if (strncmp(message->cookie, completed_reservations[reservation_id]->cookie, COOKIE_SIZE) != 0) {
                message->message_id = BAD_REQUEST;
            }
        }
    }
    else {
        if (strncmp(message->cookie, active_reservations[reservation_id]->cookie, COOKIE_SIZE) != 0) {
            message->message_id = BAD_REQUEST;
        }
    }
}

// Check if there are any active reservations that expired. If so - remove them.
void delete_expired_reservations(event_list_t *event_list) {
    auto it = active_reservations.cbegin();
    while (it != active_reservations.cend()) {
        auto reservation = it->second;
        if (uint64_t(time(nullptr)) > reservation->expiration_time) {
            event_list->events[reservation->event_id]->ticket_count += reservation->ticket_count;
            it = active_reservations.erase(it);
        }
        else it++;
    }
}

// Function reads message received from client.
// Checks the length of message and decide if and how should the server answer.
// It places information about the answer message in message_info.
size_t read_message(int socket_fd, struct sockaddr_in *client_address, char *buffer, size_t max_length,
                    message_info *message, event_list_t *event_list) {
    auto address_length = (socklen_t) sizeof(*client_address);
    int flags = 0; // we do not request anything special
    errno = 0;
    ssize_t len = recvfrom(socket_fd, buffer, max_length, flags,
                           (struct sockaddr *) client_address, &address_length);

    assert(len >= 0);

    char *pointer = buffer;

    uint8_t msg_id = *((uint8_t*) pointer);
    pointer += sizeof(uint8_t);

    message->message_id = msg_id;

    switch (msg_id) {
        case GET_EVENTS:
            if (len != sizeof(uint8_t))
                message->message_id = DONT_ANSWER;
            else
                delete_expired_reservations(event_list);
            break;
        case GET_RESERVATION:
            if (len != sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint16_t)) {
                message->message_id = DONT_ANSWER;
            }
            else {
                delete_expired_reservations(event_list);
                check_reservation_and_fill_msg_info(event_list, pointer, message);
            }
            break;
        case GET_TICKETS:
            if (len != sizeof(uint8_t) + sizeof(uint32_t) + COOKIE_SIZE * sizeof(char)) {
                message->message_id = DONT_ANSWER;
            }
            else {
                delete_expired_reservations(event_list);
                check_tickets_and_fill_msg_info(pointer, message);
            }
            break;
        default:
            // Not recognized message id so we won't answer.
            message->message_id = DONT_ANSWER;
            break;
    }

    return (size_t)len;
}

// Send message function from UDP lab scenario.
void send_message(int socket_fd, const struct sockaddr_in *client_address, void *message, size_t length) {
    auto address_length = (socklen_t) sizeof(*client_address);
    int flags = 0;
    ssize_t sent_length = sendto(socket_fd, message, length, flags,
                                 (struct sockaddr *) client_address, address_length);
    ENSURE(sent_length == (ssize_t) length);
}

// Return size of RESERVATION message size.
size_t get_reservation_msg_size() {
    return sizeof(uint8_t) + 2 * sizeof(uint32_t) + sizeof(uint16_t) + COOKIE_SIZE * sizeof(char) + sizeof(uint64_t);
}

// Return size of EVENTS message size. Save number of events to be sent in events to send.
size_t get_send_events_msg_size(event_list_t *event_list , size_t &events_to_send) {
    size_t msg_size = sizeof(uint8_t);
    events_to_send = 0;
    for (size_t i = 0; i < event_list->events_count; i++) {
        event_info *curr = event_list->events[i];
        msg_size += (sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint8_t));
        msg_size += sizeof(char) * curr->description_length;
        if (msg_size < MAX_UDP_BUFF_SIZE) {
            events_to_send++;
        }
        else {
            msg_size -= (sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint8_t));
            msg_size -= sizeof(char) * curr->description_length;
            break;
        }
    }
    return msg_size;
}

// Return size of TICKETS message.
size_t get_tickets_msg_size(uint16_t ticket_count) {
    return sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint16_t) + TICKET_SIZE * sizeof(char) * ticket_count;
}

// Send events message to the client.
// Puts all events one by one into shared buffer.
void send_events(event_list_t *event_list, int socket_fd, const struct sockaddr_in *client_address) {
    size_t events_to_send;
    size_t msg_size = get_send_events_msg_size(event_list, events_to_send);

    char *pointer = shared_buffer;
    uint8_t id = EVENTS;
    memcpy(pointer, &id, sizeof(uint8_t));
    pointer += sizeof(uint8_t);
    for (size_t i = 0; i < events_to_send; i++) {
        event_info *curr = event_list->events[i];

        uint32_t event_id_n = htobe32(curr->event_id);
        memcpy(pointer,  &event_id_n, sizeof(uint32_t));
        pointer += sizeof(uint32_t);

        uint16_t ticket_count_n = htobe16(curr->ticket_count);
        memcpy(pointer, &ticket_count_n, sizeof(uint16_t));
        pointer += sizeof(uint16_t);

        memcpy(pointer, &curr->description_length, sizeof(uint8_t));
        pointer += sizeof(uint8_t);

        memcpy(pointer, curr->description.c_str(), curr->description_length * sizeof(char));
        pointer += curr->description_length * sizeof(char);
    }
    send_message(socket_fd, client_address, (void*)shared_buffer, msg_size);
}

// Send reservation message to client. Create a reservation in active reservations.
void send_reservation(message_info *message, int socket_fd,
                      const struct sockaddr_in *client_address,
                      uint32_t reservation_id, uint32_t timeout, event_list_t *event_list) {

    create_reservation(reservation_id, message->event_id, message->ticket_count, timeout, event_list);

    auto reservation = active_reservations[reservation_id];

    size_t msg_size = get_reservation_msg_size();
    char *pointer = shared_buffer;
    uint8_t id = RESERVATION;

    memcpy(pointer, &id, sizeof(uint8_t));
    pointer += sizeof(uint8_t);

    uint32_t res_id = htobe32(reservation_id);
    memcpy(pointer, &res_id, sizeof(uint32_t));
    pointer += sizeof(uint32_t);

    uint32_t event_id = htobe32(message->event_id);
    memcpy(pointer, &event_id, sizeof(uint32_t));
    pointer += sizeof(uint32_t);

    uint32_t ticket_count = htobe16(message->ticket_count);
    memcpy(pointer, &ticket_count, sizeof(uint16_t));
    pointer += sizeof(uint16_t);

    memcpy(pointer, reservation->cookie, COOKIE_SIZE);
    pointer += COOKIE_SIZE * sizeof(char);

    uint64_t timeout_to_send = htobe64(reservation->expiration_time);
    memcpy(pointer, &timeout_to_send, sizeof(uint64_t));

    send_message(socket_fd, client_address, shared_buffer, msg_size);
}

// Send message with tickets to client.
// If reservation was active move it to completed reservations.
void send_tickets(message_info *message, int socket_fd, const sockaddr_in *client_address, ticket_t& curr_ticket) {

    bool completed;
    if (active_reservations.find(message->reservation_id) != active_reservations.end()) completed = false;
    else completed = true;

    uint16_t ticket_count;
    if (!completed) ticket_count = active_reservations[message->reservation_id]->ticket_count;
    else ticket_count = completed_reservations[message->reservation_id]->ticket_count;

    size_t msg_size = get_tickets_msg_size(ticket_count);
    char *pointer = shared_buffer;

    uint8_t id = TICKETS;
    memcpy(pointer, &id, sizeof(uint8_t));
    pointer += sizeof(uint8_t);


    uint32_t res_id = htobe32(message->reservation_id);
    memcpy(pointer, &res_id, sizeof(uint32_t));
    pointer += sizeof(uint32_t);

    ticket_count = htobe16(ticket_count);
    memcpy(pointer, &ticket_count, sizeof(uint16_t));
    pointer += sizeof(uint16_t);

    if (!completed) {
        ticket_t ticket;
        for (size_t i = 0; i < active_reservations[message->reservation_id]->ticket_count; i++) {
            ticket = curr_ticket;
            increment_ticket(curr_ticket);
            active_reservations[message->reservation_id]->tickets.push_back(ticket);
        }
        completed_reservations[message->reservation_id] = active_reservations[message->reservation_id];
        active_reservations.erase(message->reservation_id);
    }

    for (size_t i = 0; i < completed_reservations[message->reservation_id]->ticket_count; i++) {
        ticket_t ticket = completed_reservations[message->reservation_id]->tickets[i];
        memcpy(pointer, ticket.c_str(), TICKET_SIZE);
        pointer += TICKET_SIZE * sizeof(char);
    }

    send_message(socket_fd, client_address, shared_buffer, msg_size);
}

// Send bad request message to client if it is stated in message info.
void send_bad_request(message_info *message, int socket_fd,
                      const struct sockaddr_in *client_address) {
    size_t msg_size = sizeof(uint8_t) + sizeof(uint32_t);
    char *pointer = shared_buffer;
    uint8_t id = BAD_REQUEST;

    memcpy(pointer, &id, sizeof(uint8_t));
    pointer += sizeof(uint8_t);

    uint32_t error_id = htobe32(message->error_id);
    memcpy(pointer, &error_id, sizeof(uint32_t));
    send_message(socket_fd, client_address, shared_buffer, msg_size);
}

int main(int argc, char *argv[]) {
    srand(time(nullptr));
    if (argc < 3) {
        fprintf(stderr, "usage: %s -f <file> (optional: -p <port> -t <timeout>)\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // Check server parameters, if needed exit with error message.
    parametersGiven* server_parameters = check_server_parameters(argc, argv);
    printf("Listening on port %u\n", server_parameters->port);

    // Get the event list filled with events from file.
    event_list_t *event_list = event_list_from_file(server_parameters->file);

    memset(shared_buffer, 0, sizeof(shared_buffer));

    int socket_fd = bind_socket(server_parameters->port);
    struct sockaddr_in client_address{};
    size_t read_length;
    auto message = (message_info*)(malloc(sizeof(message_info)));
    // This variable will store server reservation id while server is working.
    uint32_t reservation_id = FIRST_RESERVATION_ID;
    // This variable will store server current ticket while server is working.
    ticket_t ticket = FIRST_TICKET;
    while(true) {
        read_length = read_message(socket_fd, &client_address,
                                   shared_buffer, sizeof(shared_buffer), message,
                                   event_list);
        char* client_ip = inet_ntoa(client_address.sin_addr);
        uint16_t client_port = ntohs(client_address.sin_port);
        printf("received %zd bytes from client %s:%u\n", read_length, client_ip, client_port);

        switch (message->message_id) {
            case GET_EVENTS:
                printf("Sending message containing events to client %s:%u\n", client_ip, client_port);
                send_events(event_list, socket_fd, &client_address);
                break;
            case GET_RESERVATION:
                printf("Sending message confirming reservation(%u) of %hu tickets for event %u to client %s:%u\n",
                       reservation_id, message->ticket_count, message->event_id, client_ip, client_port);
                send_reservation(message, socket_fd, &client_address,
                                 reservation_id, server_parameters->timeout, event_list);
                reservation_id++;
                break;
            case GET_TICKETS:
                printf("Sending tickets to client %s:%u\n", client_ip, client_port);
                send_tickets(message, socket_fd, &client_address, ticket);
                break;
            case BAD_REQUEST:
                printf("Sending bad request to client %s:%u\n", client_ip, client_port);
                send_bad_request(message, socket_fd, &client_address);
                break;
            default:
                printf("Received wrong message from client %s:%u\n", client_ip, client_port);
                break;
        }
    }

    CHECK_ERRNO(close(socket_fd));

    free(message);

    delete server_parameters;
    delete event_list;

    return 0;
}
