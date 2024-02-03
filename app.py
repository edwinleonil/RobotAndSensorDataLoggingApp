import socket
import sys
import time
import yaml
import csv
import os
import queue
from PyQt6.QtCore import QRunnable, QObject, pyqtSignal, QThreadPool, QRegularExpression
from PyQt6.QtWidgets import QApplication, QMainWindow, QWidget, QLabel, QLineEdit, QPushButton, QFileDialog, QGridLayout, QTextEdit
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QIntValidator, QRegularExpressionValidator
from datetime import datetime, timezone, timedelta

# from PyQt6.QtGui import *
# from PyQt6.QtWidgets import *
# from PyQt6.QtCore import *
import paho.mqtt.client as mqtt
import json


class IPValidator(QRegularExpressionValidator):
    def __init__(self, parent=None):
        super().__init__(parent)
        regex = QRegularExpression("^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$")
        self.setRegularExpression(regex)

class MyLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.validator = IPValidator(self)
        self.setValidator(self.validator)

    def validate(self):
        state = self.validator.validate(self.text(), 0)[0]
        if state == QRegularExpressionValidator.Acceptable:
            self.setStyleSheet("background-color: white;")
            return True
        else:
            self.setStyleSheet("background-color: red;")
            return False


class App(QMainWindow):
    '''Main window class'''
    def __init__(self):
        super().__init__()

        self.setWindowTitle("TCP/IP and MQTT Client App")
        self.setGeometry(100, 100, 1200, 500)
        
        # create a QThreadPool object
        self.thread_pool = QThreadPool()

        self.validator = IPValidator(self)
        

        # Center the window on the screen
        self.center()

        self.client_socket = None
        self.connected = False
        self.stop_flag = False
        self.tcpip_csv_file_path = None
        self.mqtt_csv_file_path = None
        self.tcpip_data_logging = False
        self.loggin_stop = False

        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

        self.tcpip_host = config["tcpip_address"]
        self.tcpip_port = config["tcpip_port_number"]
        self.tcpip_folder_path = config["tcpip_data_path"]

        self.mqtt_broker_address = config["mqtt_broker_address"]
        self.mqtt_broker_port = config["mqtt_broker_port"]
        self.mqtt_topic = config["mqtt_topic"]
        self.mqtt_folder_path = config["mqtt_data_path"]
        
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.tcpip_client = QLabel("TCP/IP Client")
        self.mqtt_client = QLabel("MQTT Client")

        self.tcpip_label = QLabel("TCP/IP server address:")
        self.tcpip_var = QLineEdit((self.tcpip_host))
        self.tcpip_var.setValidator(self.validator)
        

        self.tcpip_port_label = QLabel("TCP/IP tcpip_port Number:")
        self.tcpip_port_var = QLineEdit(str(self.tcpip_port))
        self.tcpip_folder_button = QPushButton("TCP/IP data save at:")
        self.tcpip_folder_var = QLineEdit(self.tcpip_folder_path)

        self.mqtt_broker_address_label = QLabel("MQTT Broker Address:")
        self.mqtt_broker_address_var = QLineEdit(self.mqtt_broker_address)
        self.mqtt_broker_address_var.setValidator(self.validator)

        self.mqtt_broker_port_label = QLabel("MQTT Broker tcpip_Port:")
        self.mqtt_broker_port_var = QLineEdit(str(self.mqtt_broker_port))
        self.mqtt_broker_port_var.setMaxLength(4)
        self.mqtt_topic_label = QLabel("MQTT Topic:")
        self.mqtt_topic_var = QLineEdit(self.mqtt_topic)
        self.mqtt_folder_button = QPushButton("MQTT data save at:")
        self.mqtt_folder_var = QLineEdit(self.mqtt_folder_path)

        self.start_button = QPushButton("Connect to TCP/IP server and MQTT brocker and log data")
        self.stop_button = QPushButton("Stop connecting / Stop logging data and disconnect / Clear status TCP/IP and MQTT status")
        self.stop_button.setEnabled(False)

        self.tcpip_status_label = QLabel("TCP/IP Status:")
        self.tcpip_status_text = QTextEdit()
        self.mqtt_status_label = QLabel("MQTT Status:")
        self.mqtt_status_text = QTextEdit()

        self.cursor = self.tcpip_status_text.textCursor()
        self.tcpip_status_text.setReadOnly(True)

        # center the text in the text box
        self.tcpip_client.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mqtt_client.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.tcpip_var.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.tcpip_port_var.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.mqtt_broker_address_var.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mqtt_broker_port_var.setAlignment(Qt.AlignmentFlag.AlignCenter)

        layout = QGridLayout()
        layout.setSpacing(10) # set spacing to 10 pixels

        layout.setColumnStretch(1, 3)
        layout.setColumnStretch(3, 3)
        layout.addWidget(self.tcpip_client, 0, 0, 1, 2)
        layout.addWidget(self.mqtt_client, 0, 2, 1, 4)

        layout.addWidget(self.tcpip_label, 1, 0)
        layout.addWidget(self.tcpip_var, 1, 1)
        layout.addWidget(self.tcpip_port_label, 2, 0)
        layout.addWidget(self.tcpip_port_var, 2, 1)

        empty_widget = QWidget()
        layout.addWidget(empty_widget, 3, 0, 1, 4)

        layout.addWidget(self.tcpip_folder_button, 5, 0)
        layout.addWidget(self.tcpip_folder_var, 5, 1,1,4)
        
        layout.addWidget(self.mqtt_broker_address_label, 1, 2)
        layout.addWidget(self.mqtt_broker_address_var, 1, 3)
        layout.addWidget(self.mqtt_broker_port_label, 2, 2)
        layout.addWidget(self.mqtt_broker_port_var, 2, 3)

        layout.addWidget(self.mqtt_topic_label, 4, 0)
        layout.addWidget(self.mqtt_topic_var, 4, 1, 1, 4)

        layout.addWidget(self.mqtt_folder_button, 6, 0)
        layout.addWidget(self.mqtt_folder_var, 6, 1,1,4)

        layout.addWidget(self.start_button, 7, 0, 1, 4)
        layout.addWidget(self.stop_button, 8, 0, 1, 4)
        layout.addWidget(self.tcpip_status_label, 9, 0)
        layout.addWidget(self.tcpip_status_text, 10, 0, 1, 2)
        layout.addWidget(self.mqtt_status_label, 9, 2)
        layout.addWidget(self.mqtt_status_text, 10, 2, 1, 4)

        self.central_widget.setLayout(layout)

        self.start_button.clicked.connect(self.on_start_button_clicked)
        self.stop_button.clicked.connect(self.stop_client)
        self.tcpip_folder_button.clicked.connect(self.select_tcpip_folder)
        self.mqtt_folder_button.clicked.connect(self.select_mqtt_folder)

    
    # center the window on the screen
    def center(self):
        height_offset = 100
        screen = QApplication.primaryScreen()
        screen_geometry = screen.geometry()
        window_geometry = self.frameGeometry()
        x = int(screen_geometry.center().x() - window_geometry.width() / 2)
        y = int(screen_geometry.center().y() - window_geometry.height() / 2)
        self.move(x, y-height_offset)

    def select_tcpip_folder(self):
        self.tcpip_folder_path = QFileDialog.getExistingDirectory(self, "Select Directory")
        self.tcpip_folder_var.setText(self.tcpip_folder_path)

    
    def select_mqtt_folder(self):
        self.mqtt_folder_path = QFileDialog.getExistingDirectory(self, "Select Directory")
        self.mqtt_folder_var.setText(self.mqtt_folder_path)

    def on_start_button_clicked(self):
        
        # get the values from the UI
        self.tcpip_host = self.tcpip_var.text()
        self.tcpip_port = int(self.tcpip_port_var.text())
        self.mqtt_broker_address = self.mqtt_broker_address_var.text()
        self.mqtt_broker_port = int(self.mqtt_broker_port_var.text())
        self.mqtt_topic = self.mqtt_topic_var.text()

        self.tcpip_folder_path = self.tcpip_folder_var.text()
        self.mqtt_folder_path = self.mqtt_folder_var.text()


        self.update_config()

        # start logging and MQTT in separate threads
        self.thread_pool.start(LogThread(self.tcpip_csv_file_path, self.tcpip_host, self.tcpip_port, self))
        self.thread_pool.start(MQTTThread(self.mqtt_broker_address, self.mqtt_broker_port, self.mqtt_topic, self.mqtt_csv_file_path, App=self))


    def update_config(self):

        with open("config.yaml",'r') as f:
            config = yaml.safe_load(f)

        config["tcpip_address"] = self.tcpip_host
        config["tcpip_port_number"] = self.tcpip_port
        config["tcpip_data_path"] = self.tcpip_folder_path
        config["mqtt_broker_address"] = self.mqtt_broker_address
        config["mqtt_broker_port"] = self.mqtt_broker_port
        config["mqtt_topic"] = self.mqtt_topic
        config["mqtt_data_path"] = self.mqtt_folder_path


        with open("config.yaml",'w') as f:
            yaml.dump(config, f)

        # close the file
        f.close()

        # list files inside the tcpip_csv_file_path directory
        tcpip_files = os.listdir(self.tcpip_folder_path)
        # list files inside the mqtt_csv_file_path directory
        mqtt_files = os.listdir(self.mqtt_folder_path)

        # remove files that execde the maximum number of files of the less numerous directory
        # remove the file with the highest number att he end of file name
        if len(tcpip_files) > len(mqtt_files):
            # remove the file with the highest number att he end of file name
            tcpip_files.sort(reverse=True)
            for i in range(len(tcpip_files)-len(mqtt_files)):
                os.remove(self.tcpip_folder_path + "/" + tcpip_files[i])
        elif len(tcpip_files) < len(mqtt_files):
            # remove the file with the highest number att he end of file name
            mqtt_files.sort(reverse=True)
            for i in range(len(mqtt_files)-len(tcpip_files)):
                os.remove(self.mqtt_folder_path + "/" + mqtt_files[i])

        # list files inside the tcpip_csv_file_path directory
        tcpip_files = os.listdir(self.tcpip_folder_path)
        # list files inside the mqtt_csv_file_path directory
        mqtt_files = os.listdir(self.mqtt_folder_path)
        
        # if files exist for tcpip
        if tcpip_files:
            # add a csv file with a consecutive number to the file containing the highest number at the end of the file name
            tcpip_files.sort(reverse=True)
            tcpip_file_number = int(tcpip_files[0].split("_")[2].split(".")[0])
            self.tcpip_csv_file_path = self.tcpip_folder_path + "/tcpip_data_" + str(tcpip_file_number+1) + ".csv"

            
        else:
            # add a csv file to the tcpip_csv_file_path directory
            self.tcpip_csv_file_path = self.tcpip_folder_path + "/tcpip_data_1.csv"
        
        # if files exist for mqtt
        if mqtt_files:
            # add a csv file with a consecutive number to the file containing the highest number at the end of the file name
            mqtt_files.sort(reverse=True)
            mqtt_file_number = int(mqtt_files[0].split("_")[2].split(".")[0])
            self.mqtt_csv_file_path = self.mqtt_folder_path + "/mqtt_data_" + str(mqtt_file_number+1) + ".csv"
            
        else:
            # add a csv file to the mqtt_csv_file_path directory
            self.mqtt_csv_file_path = self.mqtt_folder_path + "/mqtt_data_1.csv"


    def stop_client(self):
        self.stop_flag = True
        self.stop_button.setEnabled(True)
        self.start_button.setEnabled(True)
        # clear the text boxes
        self.tcpip_status_text.clear()
        self.mqtt_status_text.clear()


class WorkerSignals(QObject):
    # create signals to communicate with the main thread
    started = pyqtSignal()
    finished = pyqtSignal()
    quit = pyqtSignal()


class LogThread(QRunnable):
    # started = pyqtSignal()
    # finished = pyqtSignal()
    # quit = pyqtSignal()
    def __init__(self, tcpip_csv_file_path, tcpip_host, tcpip_port, App):

        super().__init__()

        self.app = App
        self.tcpip_csv_file_path = tcpip_csv_file_path

        self.tcpip_host = str(tcpip_host)
        self.tcpip_port = int(tcpip_port)

        self.connected = False
        self.client_socket = None
        self.logging_stop = False

        self.app.loggin_stop = False

        self.app.tcpip_data_logging = False 

        

        self.app.stop_flag = False
        self.app.start_button.setEnabled(False)
        self.app.stop_button.setEnabled(True)
        self.app.tcpip_status_text.clear()
        self.on_new_message("Connecting to TCP/IP server ...\n")
        # run update_config function to update the config.yaml file
        self.app.update_config()
    
    def on_new_message(self, message):
        # Add the new message to the text box
        self.app.tcpip_status_text.append(message)

        # Scroll the text box to the end
        scroll_bar = self.app.tcpip_status_text.verticalScrollBar()
        max_value = scroll_bar.maximum()
        height = self.app.tcpip_status_text.height()
        if max_value <= height:
            scroll_bar.setValue(height)
        else:
            scroll_bar.setValue(max_value+height)
    
    def on_stop_message(self):
        self.client_socket.close()  # close the socket
        self.on_new_message("Logging completed")            
        self.tcpip_csv_file.close()
        self.app.start_button.setEnabled(True)
        self.app.stop_button.setEnabled(True)

        # self.finished.emit()
        # self.quit.emit()
        
    def run(self):

        self.connected = False
        counter = 0

        while True:

            if self.logging_stop == True:
                break

            if self.app.stop_flag == True:
                self.on_new_message("\nConnection attempt stopped.\n")
                break
            
            if self.app.stop_flag == True:
                self.on_new_message("Client stopped\n")
                break
            
            while True:
                counter += 1

                if self.app.stop_flag == True:
                    # self.finished.emit()
                    # self.finished.emit()
                    # self.quit.emit()
                    break

                try:
                    # create a TCP/IP socket
                    self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    # set a timeout for the connection attempt
                    self.client_socket.settimeout(2)
                    # connect the socket to the server's address and tcpip_port
                    self.client_socket.connect((self.tcpip_host, self.tcpip_port))
                    self.app.start_button.setEnabled(True)
                    self.app.stop_button.setEnabled(True)

                    self.connected = True
                    self.on_new_message("Connected to TCP/IP server")       
                    break

                except ConnectionRefusedError:
                    # write on the status text box the number of connection attempts
                    self.on_new_message("Connection attempt: " + str(counter) +", Retrying in 1 second...")
                    time.sleep(1)

                except TimeoutError:
                    self.on_new_message("Connection attempt: " +str(counter) + ", timed out, check connection...")
                    time.sleep(1)


            # self.started.emit()
            
            if self.connected == True:
                self.app.stop_button.setEnabled(False)
                self.app.start_button.setEnabled(False)

                self.on_new_message("Waiting for data...\n")
                with open(self.tcpip_csv_file_path, mode='w') as self.tcpip_csv_file:
                    writer = csv.writer(self.tcpip_csv_file)
                    while True:

                        if self.app.stop_flag == True:
                            self.on_new_message("Client stopped\n")
                            break
                        
                        # send data to the server
                        message = "get data"
                        self.client_socket.sendall(message.encode('utf-8'))

                        # receive data from the server
                        data = self.client_socket.recv(1024)

                        if not data:
                            self.on_new_message("No more data, server has clossed the connection")
                            break

                        if data.decode('utf-8') == "ON":
                            self.on_new_message("Logging data...")
                            self.app.tcpip_data_logging = True
                            
                            while True:

                                # get the current timestamp in UTC
                                timestamp_utc = datetime.now(timezone.utc)
                                timestamp_str = timestamp_utc.strftime('%Y-%m-%dT%H:%M:%S.%f%z')

                                if self.app.stop_flag == True:
                                    self.on_new_message("Client stopped\n")
                                    self.client_socket.close()  # close the socket
                                    break

                                # send data to the server
                                message = "received"
                                self.client_socket.sendall(message.encode('utf-8'))

                                # receive data from the server
                                data = self.client_socket.recv(1024)
                                
                                if not data:
                                    self.on_new_message("No more data, server has clossed the connection\n")
                                    break

                                if data.decode('utf-8') == "STOP":
                                    self.logging_stop = True
                                    self.app.loggin_stop = True
                                    break
                                
                                if data.decode('utf-8') != "ON":
                                    
                                    # convert the data to a list of floats
                                    numbers = [float(x) for x in data.decode('utf-8').split(';') if x]
                                    # add a timestamp to the data
                                    numbers.insert(0, timestamp_str)
                                    writer.writerow(numbers)  # write data to CSV file              
                    
                    self.on_stop_message()

# add a class for the MQTT client that runs in a separate thread
class MQTTThread(QRunnable):
    finished = WorkerSignals()
    def __init__(self, mqtt_broker_address, mqtt_broker_port, mqtt_topic, mqtt_csv_file_path,App):
        super().__init__()

        self.broker_address = mqtt_broker_address
        self.broker_port = mqtt_broker_port
        self.topic = mqtt_topic
        self.mqtt_csv_file_path = mqtt_csv_file_path
        self.mqtt_connected = False
        self.mqtt_logging_stop = False
        self.mqtt_csv_file = None


        self.app = App
        self.app.loggin_stop = False

        # self.app.tcpip_data_logging = False 

        self.app.mqtt_status_text.clear()

        self.message_queue = queue.Queue()
        
        self.on_new_message("Connecting to MQTT broker...")

    def on_new_message(self, message):
        # Add the new message to the text box
        self.app.mqtt_status_text.append(message)

        # Scroll the text box to the end
        scroll_bar = self.app.mqtt_status_text.verticalScrollBar()
        max_value = scroll_bar.maximum()
        height = self.app.mqtt_status_text.height()
        if max_value <= height:
            scroll_bar.setValue(height)
        else:
            scroll_bar.setValue(max_value+height)

    def on_message(self, client, userdata, message):
        data = json.loads(message.payload.decode())
        parsed_data = data['timestamp'],data['position']['x'], data['position']['y'], data['position']['z'], data['position']['rx'], data['position']['ry'], data['position']['rz']
        # put the parsed data in the message queue
        self.message_queue.put(parsed_data)
    


    # function to connect to the MQTT broker
    def connect_to_broker(self, broker_address, broker_port, topic):
        
        # connect to the broker
        self.client = mqtt.Client()
        self.client.on_message = self.on_message

        self.client.connect(broker_address, broker_port, 60)
        self.client.subscribe(topic)
        # check if the connection was successful
        if self.client.is_connected():
            self.mqtt_connected = True


        self.on_new_message("Connected to MQTT broker")


    def run(self):
        
        # run the connect_to_broker function
        self.connect_to_broker(self.broker_address, self.broker_port, self.topic)
        self.on_new_message("Waiting for data...\n") 
        
        while True:
            
            if self.app.stop_flag == True:
                self.on_new_message("MQTT connection stopped by user\n")
                break
            if self.mqtt_logging_stop == True:
                break

            if self.app.tcpip_data_logging == True:         
                self.on_new_message("Logging data...\n")                
                # start the MQTT client loop
                self.client.loop_start()

                while True:

                    if self.app.stop_flag == True:
                        self.on_new_message("Client stopped\n")
                        self.client.loop_stop()
                        self.client.disconnect()
                        if self.mqtt_csv_file != None:
                            self.mqtt_csv_file.close()
                        self.mqtt_logging_stop = True
                        break
                    if self.app.loggin_stop == True:
                        self.on_new_message("Logging completed\n")
                        self.client.loop_stop()
                        self.client.disconnect()
                        # close the csv file only if it is open
                        if self.mqtt_csv_file != None:
                            self.mqtt_csv_file.close()
                        

                        self.mqtt_logging_stop = True
                        break

                    # read messages from the queue and check the condition
                    try:
                        parsed_data = self.message_queue.get(timeout=1)                        

                        # if int(parsed_data[0]) > 10:
                        with open(self.mqtt_csv_file_path, 'a', newline='') as self.mqtt_csv_file:
                            writer = csv.writer(self.mqtt_csv_file)
                            writer.writerow(parsed_data)
                    except queue.Empty:
                        self.on_new_message("No data from MQTT, check connection")
                        pass

                    time.sleep(0.1) 
            else:
                time.sleep(0.1) 

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = App()
    window.show()
    sys.exit(app.exec())    