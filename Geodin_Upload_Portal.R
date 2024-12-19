library(shiny)
library(DBI)
library(dplyr)
library(RPostgres)
library(DT)
library(readxl)
library(shinyjqui)

# Load schichten data
# setwd("C:/GIS/R")
schichten <- read_xlsx("alle_schichten.xlsx", sheet = 2, skip = 1)
petro     <- read_xlsx("alle_schichten.xlsx", sheet = 3)

# Ensure inputs are character vectors
Hauptanteil <- as.character(na.omit(petro$Hauptanteil))
Nebenanteil <- as.character(na.omit(petro$Nebenanteil))
Anteile_1      <- as.character(na.omit(petro$Anteile_1))
Anteile_2      <- as.character(na.omit(petro$Anteile_2))

# Combine all items into a single vector
all_items <- c(Hauptanteil, Nebenanteil, Anteile_1, Anteile_2)

# Extract unique values for dropdown menus
unique_depth <- sort(unique(schichten$depth), decreasing = TRUE)
unique_strat <- sort(unique(schichten$strat))
unique_genese <- sort(unique(schichten$genese))
unique_farbe <- sort(unique(schichten$farbe))
unique_zusatz <- sort(unique(schichten$zusatz))
unique_ergbem <- sort(unique(schichten$ergbem))
unique_beschbg <- sort(unique(schichten$beschbg))
unique_beschbv <- sort(unique(schichten$beschbv))
unique_gruppe <- sort(unique(schichten$gruppe))
unique_kalkgeh <- sort(unique(schichten$kalkgeh), decreasing = TRUE)
unique_bemerk <- sort(unique(schichten$bemerk))


# UI
ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      #shiny-notification-panel {
      position: fixed;
      top: 10%;
      right: 10%;
      width: 300px;
      z-index: 1050;
    }
    .shiny-notification {
      margin-bottom: 10px;
      padding: 15px;
      border: 1px solid #ccc;
      border-radius: 5px;
      box-shadow: 0px 2px 5px rgba(0,0,0,0.2);
    }
    "))
  ),
  titlePanel("GeoDIN Portal"),
  tabsetPanel(
    tabPanel(
      "Login",
      sidebarLayout(
        sidebarPanel(
          textInput("dbname", "Database Name", "geodin_bw12"),
          textInput("user", "User", "bialeks"),
          passwordInput("password", "Password", ""),
          actionButton("connect", "Connect to DB")
        ),
        mainPanel(textOutput("connection_status"))
      )
    ),
    tabPanel(
      "Filter and Data Upload",
      sidebarLayout(
        sidebarPanel(
          selectInput("longname", "Select Longname", choices = unique(ssgk$longname)),
          actionButton("filterBtn", "Filter Data"),
          actionButton("save_changes", "Save Changes"),
          hr(),
          h4("Add New Data Row"),
          selectInput("new_depth", "Depth", choices = unique_depth),
          selectInput("new_strat", "Strat", choices = unique_strat),
          selectInput("new_genese", "Genese", choices = unique_genese),
          selectInput("new_farbe", "Farbe", choices = unique_farbe),
          selectInput("new_zusatz", "Zusatz", choices = unique_zusatz),
          selectInput("new_ergbem", "Ergbem", choices = unique_ergbem),
          selectInput("new_beschbg", "Beschbg", choices = unique_beschbg),
          selectInput("new_beschbv", "Beschbv", choices = unique_beschbv),
          selectInput("new_kalkgeh", "Kalkgeh", choices = unique_kalkgeh),
          selectInput("new_bemerk", "Bemerk", choices = unique_bemerk),
          actionButton("addRowBtn", "Add Row"),
          h3("Adjust Petro Values Using Drag-and-Drop"),
          fluidRow(
            column(
              6,
              h4("Source"),
              # Separate categories for the source
              h5("Hauptanteil"),
              orderInput('hauptanteil_source', '', items = Hauptanteil, as_source = TRUE, connect = 'combined_dest'),
              h5("Nebenanteil"),
              orderInput('nebenanteil_source', '', items = Nebenanteil, as_source = TRUE, connect = 'combined_dest'),
              h5("Anteile_1"),
              orderInput('anteile1_source', '', items = Anteile_1, as_source = TRUE, connect = 'combined_dest'),
              h5("Anteile_2"),
              orderInput('anteile2_source', '', items = Anteile_2, as_source = TRUE, connect = 'combined_dest')
            ),
            column(
              6,
              h4("Order Values"),
              # Combined destination for reordering
              orderInput('combined_dest', '', items = NULL, placeholder = 'Order Values here...')
            )
          ),
          textOutput("petro_value")
        ),
        mainPanel(DTOutput("geoData"))
      )
    )
  )
)

server <- function(input, output, session) {
  
  # Reactive values for storing data and temporary edits
  rv <- reactiveValues(data = NULL)
  
  # Reactive values to store database connections separately
  postgres_connection <- reactiveVal(NULL)
  sqlite_connection <- reactiveVal(NULL)
  
  # Output connection status
  output$connection_status <- renderText({
    if (is.null(postgres_connection()) && is.null(sqlite_connection())) {
      "Not connected"
    } else if (!is.null(postgres_connection())) {
      "Connected to PostgreSQL"
    } else {
      "Connected to SQLite"
    }
  })
  
  # Handle database connection on 'Connect' button click
  observeEvent(input$connect, {
    tryCatch({
      # Attempt to connect to PostgreSQL
      pg_conn <- tryCatch({
        dbConnect(
          RPostgres::Postgres(),
          host = "bchdcgissql01.cdmeu.internal.cdm.com",
          dbname = input$dbname,
          port = 5432,
          user = input$user,
          password = input$password
        )
      }, error = function(e) {
        showNotification("Failed to connect to PostgreSQL.", type = "warning", duration = 5)
        NULL
      })
      
      # Store PostgreSQL connection
      if (!is.null(pg_conn)) {
        postgres_connection(pg_conn)
        showNotification("PostgreSQL connection established successfully!", type = "message", duration = 5)
        
        # Set timezone for PostgreSQL session
        dbExecute(pg_conn, "SET TIME ZONE 'Europe/Berlin';")
      }
      
      # Attempt to connect to SQLite
      con_lite <- tryCatch({
        dbConnect(RSQLite::SQLite(), "C:/GIS/R/offline.db")
      }, error = function(e) {
        showNotification("Failed to connect to SQLite.", type = "warning", duration = 5)
        NULL
      })
      
      # Store SQLite connection
      if (!is.null(con_lite)) {
        sqlite_connection(con_lite)
        showNotification("SQLite connection established successfully!", type = "message", duration = 5)
      }
      
      # Check if both connections are successful
      if (!is.null(pg_conn) && !is.null(con_lite)) {
        
        # Verbindung zu SQLite herstellen
        sqlite_conn <- con_lite
        
        # Abfragen für die Tabellen erstellen
        query_ssgkrzt1 <- "SELECT * FROM geodin_loc_ssgkrzt1 WHERE action IS NOT NULL;"
        query_ssgprobe <- "SELECT * FROM geodin_loc_ssgprobe WHERE action IS NOT NULL;"
        query_ssgsvgeo <- "SELECT * FROM geodin_loc_ssgsvgeo WHERE action IS NOT NULL;"
        
        # Funktion zum Verarbeiten von UPDATE-Daten
        process_update <- function(data, remote_table) {
          for (i in seq_len(nrow(data))) {
            row_id <- data[i, "ogc_fid"]
            set_clause <- sapply(names(data), function(col) {
              new_value <- data[i, col]
              if (!is.na(new_value) && col != "ogc_fid" && col != "action" && col != "ogc_fid1") {
                # Prüfen, ob die Spalte ein Datum ist, und konvertieren
                if (col %in% c("bearb_dat", "bohrz_von", "bohrz_bis") && !is.na(new_value)) {
                  paste0(DBI::dbQuoteIdentifier(pg_conn, col), " = ", 
                         DBI::dbQuoteLiteral(pg_conn, as.Date(new_value, origin = "1970-01-01")))
                } else {
                  paste0(DBI::dbQuoteIdentifier(pg_conn, col), " = ", DBI::dbQuoteLiteral(pg_conn, new_value))
                }
              } else {
                NULL
              }
            })
            
            set_clause <- set_clause[!sapply(set_clause, is.null)]
            if (length(set_clause) > 0) {
              query <- sprintf(
                "UPDATE %s SET %s WHERE ogc_fid = %s",
                remote_table,
                paste(set_clause, collapse = ", "),
                DBI::dbQuoteLiteral(pg_conn, row_id)
              )
              tryCatch({
                dbExecute(pg_conn, query)
              }, error = function(e) {
                print(paste("Fehler beim Aktualisieren in PostgreSQL (", remote_table, "):", sep=""))
                print(e)
              })
            }
          }
        }
        
        # Funktion zum Verarbeiten von INSERT-Daten
        process_insert <- function(data, remote_table) {
          for (i in seq_len(nrow(data))) {
            insert_columns <- names(data)[!names(data) %in% c("ogc_fid", "action", "ogc_fid1")]
            insert_values <- sapply(insert_columns, function(col) {
              value <- data[i, col]
              # Prüfen, ob die Spalte ein Datum ist, und konvertieren
              if (col %in% c("bearb_dat", "bohrz_von", "bohrz_bis") && !is.na(value)) {
                DBI::dbQuoteLiteral(pg_conn, as.Date(value, origin = "1970-01-01"))
              } else if (!is.na(value)) {
                DBI::dbQuoteLiteral(pg_conn, value)
              } else {
                "NULL"
              }
            })
            
            query <- sprintf(
              "INSERT INTO %s (%s) VALUES (%s)",
              remote_table,
              paste(DBI::dbQuoteIdentifier(pg_conn, insert_columns), collapse = ", "),
              paste(insert_values, collapse = ", ")
            )
            tryCatch({
              dbExecute(pg_conn, query)
            }, error = function(e) {
              print(paste("Fehler beim Einfügen in PostgreSQL (", remote_table, "):", sep=""))
              print(e)
            })
          }
        }
        
        
        # Funktion zum Synchronisieren von Daten nach erfolgreicher Übertragung
        sync_tables <- function(sqlite_table, postgres_table) {
          tryCatch({
            # SQLite-Tabelle leeren
            dbExecute(sqlite_conn, sprintf("DELETE FROM %s;", sqlite_table))
            
            # Autoincrement zurücksetzen
            dbExecute(sqlite_conn, sprintf("DELETE FROM sqlite_sequence WHERE name='%s';", sqlite_table))
            
            # PostgreSQL-Tabelle abrufen
            data <- dbGetQuery(pg_conn, sprintf("SELECT * FROM %s;", postgres_table))
            
            # Daten in SQLite-Tabelle einfügen
            dbWriteTable(sqlite_conn, sqlite_table, data, append = TRUE, row.names = FALSE)
            print(paste("Tabelle", sqlite_table, "erfolgreich synchronisiert."))
          }, error = function(e) {
            print(paste("Fehler beim Synchronisieren der Tabelle:", sqlite_table))
            print(e)
          })
        }
        
        # Daten aus jeder Tabelle abrufen und verarbeiten
        process_data <- function(query, remote_table) {
          data <- tryCatch({
            dbGetQuery(sqlite_conn, query)
          }, error = function(e) {
            print(paste("Fehler beim Abrufen der Daten aus SQLite:", remote_table))
            print(e)
            NULL
          })
          
          if (!is.null(data)) {
            update_data <- data[!is.na(data$ogc_fid), ]
            insert_data <- data[is.na(data$ogc_fid), ]
            
            if (nrow(update_data) > 0) {
              process_update(update_data, remote_table)
            }
            
            if (nrow(insert_data) > 0) {
              process_insert(insert_data, remote_table)
            }
          }
        }
        
        print("Abrufen und Verarbeiten von Daten aus geodin_loc_ssgkrzt1...")
        process_data(query_ssgkrzt1, "public.geodin_loc_ssgkrzt1")
        sync_tables("geodin_loc_ssgkrzt1", "public.geodin_loc_ssgkrzt1")
        
        print("Abrufen und Verarbeiten von Daten aus geodin_loc_ssgprobe...")
        process_data(query_ssgprobe, "public.geodin_loc_ssgprobe")
        sync_tables("geodin_loc_ssgprobe", "public.geodin_loc_ssgprobe")
        
        print("Abrufen und Verarbeiten von Daten aus geodin_loc_ssgsvgeo...")
        process_data(query_ssgsvgeo, "public.geodin_loc_ssgsvgeo")
        sync_tables("geodin_loc_ssgsvgeo", "public.geodin_loc_ssgsvgeo")
        
        
        
        showNotification("PostgreSQL and SQLite have been successfully synchronized!", type = "message", duration = 5)
        
        # Insert your additional code here
      } else if (is.null(pg_conn) && is.null(con_lite)) {
        showNotification("Failed to connect to any database.", type = "error", duration = 5)
      }
      
    }, error = function(e) {
      # Cleanup in case of failure
      postgres_connection(NULL)
      sqlite_connection(NULL)
      showNotification("Failed to connect to any database.", type = "error", duration = 5)
    })
  })
  
  # Combine petro values reactively
  petro <- reactive({
    # Extract the items from the drag-and-drop destination (combined_dest)
    selected_items <- input$combined_dest
    
    # Initialize variables
    prefix_combined <- ""  # For Anteile_2
    main_combined <- ""    # For Hauptanteil
    suffix_combined <- ""  # For Anteile_1
    final_combined <- ""   # For everything else
    
    # Loop through the selected items
    for (item in selected_items) {
      if (item %in% Anteile_2) {
        # If the item is from Anteile_2, add to the prefix (before Hauptanteil)
        prefix_combined <- paste0(prefix_combined, item)
      } else if (item %in% Hauptanteil) {
        # If the item is from Hauptanteil, add to the main_combined
        if (main_combined != "") {
          main_combined <- paste0(main_combined, ", ", item)
        } else {
          main_combined <- paste0(main_combined, item)
        }
      } else if (item %in% Anteile_1) {
        # If the item is from Anteile_1, add to suffix (directly after Hauptanteil)
        suffix_combined <- paste0(suffix_combined, item)
      } else {
        # For all other items, add to the final_combined with commas
        if (final_combined != "") {
          final_combined <- paste0(final_combined, ", ", item)
        } else {
          final_combined <- paste0(final_combined, item)
        }
      }
    }
    
    # Concatenate all parts in the desired order:
    # Anteile_2 (prefix) → Hauptanteil → Anteile_1 (suffix) → Everything else
    final_result <- paste0(prefix_combined, main_combined, suffix_combined, if (final_combined != "") paste0(", ", final_combined))
    
    # Return the final result
    return(final_result)
  })
  
  # Display the reordered items in the textfield
  output$petro_value <- renderText({
    petro()
  })
  
  
  
  # Filter data based on selected longname
  filtered_data <- eventReactive(input$filterBtn, {
    req(input$longname)
    
    con_lite <- sqlite_connection()
    ssgk_key <- dbGetQuery(con_lite, paste0(
      "SELECT prj_id, locid FROM geodin_loc_ssgkrzt1 WHERE longname = '", input$longname, "'"
    ))
    
    validate(
      need(nrow(ssgk_key) > 0, "No matching records found in ssgk table.")
    )
    
    geo <- dbGetQuery(con_lite, "SELECT * FROM geodin_loc_ssgsvgeo")
    geo_filtered <- geo %>%
      filter(prj_id %in% ssgk_key$prj_id, locid %in% ssgk_key$locid) %>%
      select(depth, strat, petro, genese, farbe, zusatz, ergbem, beschbg, beschbv, kalkgeh, bemerk, action, ogc_fid1)
    
    rv$data <- geo_filtered
    geo_filtered  # Return fresh data from the database
  })
  
  # Render filtered data in a DataTable
  # Render filtered data in a DataTable
  output$geoData <- renderDT({
    data <- filtered_data()
    if (nrow(data) == 0) {
      data.frame(Message = "No records match the selected criteria.")
    } else {
      datatable(data %>% select(-action, -ogc_fid1), editable = TRUE, options = list(
        pageLength = 10,
        autoWidth = TRUE,
        scrollX = TRUE
      )) 
    }
  })
  
  
  # Handle edits in the DataTable
  observeEvent(input$geoData_cell_edit, {
    info <- input$geoData_cell_edit
    i <- info$row
    j <- info$col
    v <- info$value
    
    # Ensure temporary data exists
    if (is.null(rv$temp_data)) {
      rv$temp_data <- rv$data
    }
    
    # Update the edited cell value
    rv$temp_data[i, j] <- DT::coerceValue(v, rv$data[i, j])
    
    # Only set action to UPDATE if the "action" field is not already occupied
    if (is.na(rv$temp_data[i, "action"]) || rv$temp_data[i, "action"] == "") {
      rv$temp_data[i, "action"] <- "UPDATE"
    }
  })
  
  # Save changes to the database
  observeEvent(input$save_changes, {
    req(rv$temp_data, db_connection())
    
    con <- con_lite  # Use the SQLite connection
    success <- TRUE
    
    for (i in seq_len(nrow(rv$temp_data))) {
      row_id <- rv$temp_data[i, "ogc_fid1"]
      if (is.na(row_id) || row_id == "") {
        success <- FALSE
        next
      }
      
      set_clause <- sapply(names(rv$temp_data), function(col) {
        new_value <- rv$temp_data[i, col]
        old_value <- rv$data[i, col]
        if (!is.na(new_value) && !identical(new_value, old_value)) {
          paste0(DBI::dbQuoteIdentifier(con, col), " = ", DBI::dbQuoteLiteral(con, new_value))
        } else {
          NULL
        }
      })
      
      set_clause <- set_clause[!sapply(set_clause, is.null)]
      if (length(set_clause) > 0) {
        query <- sprintf(
          "UPDATE geodin_loc_ssgsvgeo SET %s WHERE ogc_fid1 = %d",
          paste(set_clause, collapse = ", "),
          row_id
        )
        tryCatch({
          dbExecute(con, query)
        }, error = function(e) {
          success <- FALSE
        })
      }
    }
    
    if (success) {
      showNotification("Changes saved successfully! :) ", type = "message", duration = 5)
    } else {
      showNotification("Changes not saved successfully :(.", type = "error", duration = 5)
    }
  })
  
  
  # Add a new row to the database
  observeEvent(input$addRowBtn, {
    req(input$longname, db_connection())
    
    con_lite <- sqlite_connection()
    ssgk_key <- dbGetQuery(con_lite, paste0(
      "SELECT prj_id, locid FROM geodin_loc_ssgkrzt1 WHERE longname = '",
      input$longname, "'"
    ))
    
    # Check if the action column exists
    columns <- dbListFields(con_lite, "geodin_loc_ssgsvgeo")
    if (!"action" %in% columns) {
      stop("The 'action' column does not exist in the 'geodin_loc_ssgsvgeo' table.")
    }
    
    max_recid <- dbGetQuery(con_lite, paste0(
      "SELECT MAX(recid) AS max_recid FROM geodin_loc_ssgsvgeo WHERE prj_id = '",
      ssgk_key$prj_id[1], "' AND locid = ", ssgk_key$locid[1]
    ))$max_recid
    
    
    new_recid <- ifelse(is.na(max_recid), 1, max_recid + 1)
    
    
    # Check if a valid match is found
    validate(
      need(nrow(ssgk_key) > 0, "No matching records found in ssgk table.")
    )
    
    # Create a new row using the input values
    new_row <- data.frame(
      prj_id = ssgk_key$prj_id[1],
      locid = ssgk_key$locid[1],
      recid = new_recid,  # Use the incremented recid value
      depth = input$new_depth,
      strat = input$new_strat,
      petro = petro(),  # Use the reactive petro value
      genese = input$new_genese,
      farbe = input$new_farbe,
      zusatz = input$new_zusatz,
      ergbem = input$new_ergbem,
      beschbg = input$new_beschbg,
      beschbv = input$new_beschbv,
      kalkgeh = input$new_kalkgeh,
      bemerk = input$new_bemerk,
      action = "INSERT"  # Set action to INSERT
    )
    
    print(str(new_row))  # Print the structure of new_row
    
    # Construct the INSERT query
    query <- sprintf(
      "INSERT INTO geodin_loc_ssgsvgeo (%s) VALUES (%s)",
      paste(names(new_row), collapse = ", "),
      paste(sapply(new_row, function(x) DBI::dbQuoteLiteral(con_lite, x)), collapse = ", ")
    )
    
    print(query)  # Print the query
    
    # Execute the query and handle any errors
    tryCatch({
      dbExecute(con_lite, query)
      showNotification("Row added successfully!", type = "message", duration = 5)
    }, error = function(e) {
      showNotification(paste("Failed to add row:", e$message), type = "error", duration = 5)
      
    })
  })
}


shinyApp(ui, server)
