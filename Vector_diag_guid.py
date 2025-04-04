import sys
import os
import csv
import traceback
import uuid
from datetime import datetime
import time # Cho việc sleep nhỏ trong thread

# --- Kiểm tra và Nhập Thư viện ---
try:
    import cantools
except ImportError:
    print("Lỗi: Thư viện 'cantools' chưa được cài đặt.")
    sys.exit(1)

try:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
        QAction, QFileDialog, QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
        QStatusBar, QMessageBox, QSplitter, QHeaderView, QLabel, QMenuBar,
        QTabWidget, QPushButton, QLineEdit, QComboBox, QCheckBox, QSizePolicy,
        QProgressDialog # Để hiển thị quá trình quét kênh
    )
    from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject, QSize, QTimer, QMutex, QMutexLocker
    from PyQt5.QtGui import QIcon, QFont, QColor
except ImportError:
    print("Lỗi: Thư viện 'PyQt5' chưa được cài đặt.")
    sys.exit(1)

try:
    import can
    from can.interfaces.vector import VectorError # Bắt lỗi cụ thể của Vector
    PYTHON_CAN_AVAILABLE = True
except ImportError:
    print("Lỗi: Thư viện 'python-can' chưa được cài đặt hoặc có lỗi.")
    print("Vui lòng cài đặt bằng lệnh: pip install python-can")
    PYTHON_CAN_AVAILABLE = False
    # sys.exit(1) # Có thể thoát hoặc để ứng dụng chạy với tính năng hạn chế

# Tùy chọn: Thư viện đồ thị
try:
    import pyqtgraph as pg
    PYQTGRAPH_AVAILABLE = True
except ImportError:
    PYQTGRAPH_AVAILABLE = False
    print("Cảnh báo: Thư viện 'pyqtgraph' không có sẵn. Tab đồ thị sẽ bị vô hiệu hóa.")

# --- Định nghĩa Hằng số ---
DEFAULT_BAUD_RATES = ["1000000", "500000", "250000", "125000", "100000", "50000"]
DEFAULT_DATA_BAUD_RATES = ["8000000", "5000000", "4000000", "2000000", "1000000"] # Cho CAN FD

CONNECTION_STATUS = {
    "offline": ("Offline", QColor("gray")),
    "online": ("Online", QColor("green")),
    "connecting": ("Connecting...", QColor("orange")),
    "error": ("Error", QColor("red")),
    "scanning": ("Scanning...", QColor("blue")),
}

# --- Worker Threads (DbcLoadingWorker, TraceLoadingWorker giữ nguyên từ bản trước) ---
# Thêm CanListenerThread

class DbcLoadingWorker(QThread): # Giữ nguyên
    finished = pyqtSignal(str, object, str) # network_id, db, path_or_error
    progress = pyqtSignal(str, str)
    # ... (code giống bản trước) ...
    def __init__(self, network_id, file_path):
        super().__init__()
        self.network_id = network_id
        self.file_path = file_path
    def run(self):
        # (Code from previous version)
        try:
            self.progress.emit(self.network_id, f"Phân tích DBC: {os.path.basename(self.file_path)}...")
            db = cantools.db.load_file(self.file_path, strict=False, encoding='latin-1') # Or try multiple encodings
            self.finished.emit(self.network_id, db, self.file_path)
        except FileNotFoundError:
            self.finished.emit(self.network_id, None, f"Error: DBC file not found '{self.file_path}'")
        except Exception as e:
             error_details = traceback.format_exc()
             self.finished.emit(self.network_id, None, f"Error reading DBC:\n{e}\n\nDetails:\n{error_details}")

class TraceLoadingWorker(QThread): # Giữ nguyên
    finished = pyqtSignal(str, list, dict, str) # network_id, trace_data, signal_timeseries, path_or_error
    progress = pyqtSignal(str, str)
    progress_percent = pyqtSignal(str, int)
    # ... (code giống bản trước) ...
    def __init__(self, network_id, file_path, db=None):
        super().__init__()
        self.network_id = network_id
        self.file_path = file_path
        self.db = db
    def run(self):
        # (Code from previous version - parses CSV)
        # Emits finished signal with parsed data or errors
        trace_data = []
        signal_timeseries = {}
        try:
             # Simplified parsing logic
             with open(self.file_path, 'r', encoding='utf-8') as f:
                 # ... (parsing logic for timestamp, id, dlc, data) ...
                 # Simulate loading completion
                 time.sleep(1) # Simulate work for demonstration
                 self.progress.emit(self.network_id, "Trace file read complete.")
                 self.progress_percent.emit(self.network_id, 100)
             self.finished.emit(self.network_id, trace_data, signal_timeseries, self.file_path)
        except FileNotFoundError:
            self.finished.emit(self.network_id, None, {}, f"Error: Trace file not found '{self.file_path}'")
        except Exception as e:
             error_details = traceback.format_exc()
             self.finished.emit(self.network_id, None, {}, f"Error reading Trace file:\n{e}\n\nDetails:\n{error_details}")

class LoggingWorker(QThread): # Giữ nguyên
    error = pyqtSignal(str, str) # network_id, error_message
    status = pyqtSignal(str, str) # network_id, status_message
    message_count = pyqtSignal(str, int) # net_id, count

    # ... (code gần giống bản trước, có message_queue, mutex) ...
    def __init__(self, network_id, log_file_path):
        super().__init__()
        self.network_id = network_id
        self.log_file_path = log_file_path
        self._is_running = False
        self.message_queue = [] # Simple list-based queue for demo
        self.queue_mutex = QMutex()
        self.writer = None
        self.file = None
        self._message_counter = 0

    def run(self):
        self._is_running = True
        self._message_counter = 0
        try:
            self.status.emit(self.network_id, f"Starting log: {os.path.basename(self.log_file_path)}")
            # Use 'a' to append if file exists? Or 'w' to overwrite?
            self.file = open(self.log_file_path, 'w', newline='', encoding='utf-8')
            self.writer = csv.writer(self.file)
            self.writer.writerow(['Timestamp', 'ID_Hex', 'DLC', 'Data_Hex', 'IsExtended', 'IsRemote', 'IsError']) # Include more message attrs

            while self._is_running:
                messages_to_write = []
                with QMutexLocker(self.queue_mutex):
                    if self.message_queue:
                        messages_to_write = self.message_queue
                        self.message_queue = [] # Clear the queue

                if messages_to_write:
                    for msg in messages_to_write:
                        # Format can.Message for CSV
                        row = [
                            f"{msg.timestamp:.6f}", # Timestamp with higher precision
                            f"{msg.arbitration_id:X}",
                            str(msg.dlc),
                            msg.data.hex().upper(),
                            str(msg.is_extended_id),
                            str(msg.is_remote_frame),
                            str(msg.is_error_frame),
                            # Add channel? msg.channel?
                        ]
                        self.writer.writerow(row)
                        self._message_counter +=1
                    self.file.flush() # Flush occasionally
                    self.message_count.emit(self.network_id, self._message_counter) # Emit count


                # Sleep only if the queue was empty to avoid delaying writes
                if not messages_to_write:
                     self.msleep(50) # Check queue periodically

            self.status.emit(self.network_id, "Logging stopped.")

        except Exception as e:
            self.error.emit(self.network_id, f"Logging Error: {e}")
            self.status.emit(self.network_id, "Logging Error.")
        finally:
            if self.file:
                self.file.close()
            self._is_running = False
            self.message_count.emit(self.network_id, self._message_counter) # Final count

    def stop(self):
        self.status.emit(self.network_id, "Stopping log...")
        self._is_running = False

    def add_message(self, message: can.Message):
         """Adds a single message to the queue (thread-safe)."""
         if self._is_running:
              with QMutexLocker(self.queue_mutex):
                   self.message_queue.append(message)

    def add_messages(self, messages: list):
        """Adds a list of messages to the queue (thread-safe)."""
        if self._is_running:
            with QMutexLocker(self.queue_mutex):
                self.message_queue.extend(messages)

class CanListenerThread(QThread):
    """Thread to receive messages from a python-can bus."""
    message_received = pyqtSignal(str, object) # network_id, can.Message object
    listener_error = pyqtSignal(str, str)      # network_id, error message
    connection_closed = pyqtSignal(str)       # network_id

    def __init__(self, network_id, can_bus: can.Bus, parent=None):
        super().__init__(parent)
        self.network_id = network_id
        self.bus = can_bus
        self._is_running = False

    def run(self):
        self._is_running = True
        print(f"Listener thread started for network {self.network_id}")
        while self._is_running:
            try:
                # Use a timeout to allow checking _is_running flag
                msg = self.bus.recv(timeout=0.1)
                if msg:
                    # Add network_id or potentially channel info to the message if needed later
                    # msg.network_id = self.network_id
                    self.message_received.emit(self.network_id, msg)
                # else:
                    # Timeout occurred, loop continues

                # Add a small sleep to prevent high CPU usage if recv is non-blocking immediately
                # self.msleep(1) # Careful: this adds latency

            except can.CanError as e:
                 # Handle specific CAN errors (e.g., bus detached)
                 self.listener_error.emit(self.network_id, f"CAN Bus Error: {e}")
                 self._is_running = False # Stop the thread on critical bus error
            except Exception as e:
                 # Handle unexpected errors
                 error_details = traceback.format_exc()
                 self.listener_error.emit(self.network_id, f"Listener Error: {e}\n{error_details}")
                 self._is_running = False # Stop on unexpected error

        print(f"Listener thread stopping for network {self.network_id}")
        try:
             # Optionally, ensure bus cleanup if not done elsewhere
             # self.bus.shutdown() # Careful: shutdown might be called by main thread
             pass
        except Exception as e:
            print(f"Error during listener ({self.network_id}) cleanup: {e}")
        finally:
            self.connection_closed.emit(self.network_id) # Signal that the listener has fully stopped

    def stop(self):
        print(f"Requesting stop for listener thread {self.network_id}")
        self._is_running = False

# --- Widgets cho các Tab ---

class BaseNetworkTab(QWidget): # Giữ nguyên cơ sở
    loadDbcRequested = pyqtSignal(str)
    loadTraceRequested = pyqtSignal(str)
    selectLogFileRequested = pyqtSignal(str)
    toggleLoggingRequested = pyqtSignal(str)
    connectRequested = pyqtSignal(str)      # NEW: Request connect
    disconnectRequested = pyqtSignal(str)   # NEW: Request disconnect
    rescanChannelsRequested = pyqtSignal()  # NEW: Request channel rescan

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_network_id = None
        self.network_data = {}

    def update_content(self, network_id, network_data, available_channels=None): # Add available_channels
        self.current_network_id = network_id
        self.network_data = network_data # Shallow copy is fine here

# Tab Cấu hình DBC (không đổi nhiều)
class DbcConfigTab(BaseNetworkTab):
    def __init__(self, parent=None):
        super().__init__(parent)
        # ... (Layout giống bản trước) ...
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        self.dbcPathLabel = QLabel("File DBC: Chưa tải")
        self.dbcPathLabel.setWordWrap(True)
        self.loadDbcButton = QPushButton(QIcon.fromTheme("document-open"), "Tải / Thay đổi DBC...")
        self.dbcStructureTree = QTreeWidget()
        # ... _setup_dbc_tree_widget()...
        self._setup_dbc_tree_widget()
        layout.addWidget(self.dbcPathLabel)
        layout.addWidget(self.loadDbcButton)
        layout.addWidget(self.dbcStructureTree)
        self.loadDbcButton.clicked.connect(self._request_load_dbc)

    def _setup_dbc_tree_widget(self): # Giống bản trước
        # ... (code giống bản trước) ...
        headers = [ "Name / Desc", "ID (Hex)", "DLC", "Sender(s)", "Start Bit", "Length", "Byte Order", "Type", "Factor", "Offset", "Unit", "Receivers", "Comment" ]
        self.dbcStructureTree.setColumnCount(len(headers))
        self.dbcStructureTree.setHeaderLabels(headers)
        self.dbcStructureTree.header().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.dbcStructureTree.header().setSectionResizeMode(0, QHeaderView.Stretch)

    def _request_load_dbc(self):
        if self.current_network_id:
            self.loadDbcRequested.emit(self.current_network_id)

    def update_content(self, network_id, network_data, available_channels=None): # Chấp nhận available_channels nhưng không dùng
        super().update_content(network_id, network_data)
        # ... (logic cập nhật label và populate tree giống bản trước) ...
        dbc_path = network_data.get('dbc_path', None)
        db = network_data.get('db', None)
        if dbc_path: self.dbcPathLabel.setText(f"File DBC: {dbc_path}")
        else: self.dbcPathLabel.setText("File DBC: Chưa tải")
        self.dbcStructureTree.clear()
        if db: self.populate_dbc_tree(db)

    def populate_dbc_tree(self, db): # Giống bản trước
         # ... (code populate giống bản trước) ...
         self.dbcStructureTree.clear()
         # Group by Nodes -> Messages -> Signals (simplified example)
         # ... logic to add items ...


# --- NEW Tab: Cấu hình Phần cứng ---
class HardwareConfigTab(BaseNetworkTab):
    configChanged = pyqtSignal(str, str, object) # net_id, key, value

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QGridLayout(self) # Dùng Grid layout cho đẹp
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        row = 0
        # --- Trạng thái Kết nối ---
        layout.addWidget(QLabel("Trạng thái:"), row, 0)
        self.statusLabel = QLabel("Offline")
        font = self.statusLabel.font()
        font.setBold(True)
        self.statusLabel.setFont(font)
        self.update_status_display("offline") # Set màu ban đầu
        layout.addWidget(self.statusLabel, row, 1, 1, 2) # Span 2 cột

        row += 1
        # --- Chọn Kênh ---
        layout.addWidget(QLabel("Kênh Vector:"), row, 0)
        self.channelCombo = QComboBox()
        layout.addWidget(self.channelCombo, row, 1)
        self.rescanButton = QPushButton(QIcon.fromTheme("view-refresh"), "Quét lại Kênh")
        self.rescanButton.clicked.connect(self._request_rescan)
        layout.addWidget(self.rescanButton, row, 2)

        row += 1
        # --- Cài đặt Kênh ---
        layout.addWidget(QLabel("Baud Rate:"), row, 0)
        self.baudrateCombo = QComboBox()
        self.baudrateCombo.addItems(DEFAULT_BAUD_RATES)
        self.baudrateCombo.setEditable(True) # Cho phép nhập giá trị tùy chỉnh
        layout.addWidget(self.baudrateCombo, row, 1, 1, 2)

        row += 1
        # --- Cài đặt CAN FD ---
        self.fdCheckbox = QCheckBox("Kích hoạt CAN FD")
        layout.addWidget(self.fdCheckbox, row, 0, 1, 3) # Span 3 cột

        row += 1
        layout.addWidget(QLabel("Data Baud Rate:"), row, 0)
        self.dataBaudrateCombo = QComboBox()
        self.dataBaudrateCombo.addItems(DEFAULT_DATA_BAUD_RATES)
        self.dataBaudrateCombo.setEditable(True)
        layout.addWidget(self.dataBaudrateCombo, row, 1, 1, 2)

        # Disable FD settings initially
        self.dataBaudrateCombo.setEnabled(False)
        self.fdCheckbox.toggled.connect(self.dataBaudrateCombo.setEnabled)

        # --- Kết nối / Ngắt Kết nối ---
        row += 1
        self.connectButton = QPushButton(QIcon.fromTheme("network-connect"), "Kết nối")
        self.connectButton.setStyleSheet("QPushButton { background-color: lightgreen; }")
        layout.addWidget(self.connectButton, row, 1)

        self.disconnectButton = QPushButton(QIcon.fromTheme("network-disconnect"), "Ngắt Kết nối")
        self.disconnectButton.setStyleSheet("QPushButton { background-color: lightcoral; }")
        self.disconnectButton.setEnabled(False) # Disable initially
        layout.addWidget(self.disconnectButton, row, 2)

        layout.setRowStretch(row + 1, 1) # Đẩy mọi thứ lên trên

        # --- Kết nối Signals ---
        self.channelCombo.currentIndexChanged.connect(lambda: self._emit_config_change('interface_channel', self.channelCombo.currentData()))
        self.baudrateCombo.currentTextChanged.connect(lambda text: self._emit_config_change('baud_rate', text))
        self.fdCheckbox.toggled.connect(lambda checked: self._emit_config_change('is_fd', checked))
        self.dataBaudrateCombo.currentTextChanged.connect(lambda text: self._emit_config_change('data_baud_rate', text))
        self.connectButton.clicked.connect(self._request_connect)
        self.disconnectButton.clicked.connect(self._request_disconnect)


    def _emit_config_change(self, key, value):
         """Emit signal when a config widget changes, validating numeric inputs."""
         if key in ['baud_rate', 'data_baud_rate']:
              try:
                  # Chỉ emit nếu là số hợp lệ
                  int_value = int(value)
                  if int_value > 0:
                       self.configChanged.emit(self.current_network_id, key, int_value)
                  # else: Optionally handle non-positive numbers silently or with a warning
              except ValueError:
                  # Ignore non-integer input for baud rates temporarily
                  # Could show visual feedback to the user
                   pass
         else: # For channel selection and checkbox
              self.configChanged.emit(self.current_network_id, key, value)

    def _request_connect(self):
        if self.current_network_id:
            self.connectRequested.emit(self.current_network_id)

    def _request_disconnect(self):
        if self.current_network_id:
            self.disconnectRequested.emit(self.current_network_id)

    def _request_rescan(self):
         # Chỉ cần emit signal, MainWindow sẽ xử lý
         self.rescanChannelsRequested.emit()


    def update_content(self, network_id, network_data, available_channels=None):
        super().update_content(network_id, network_data)

        # --- Cập nhật danh sách kênh ---
        # Block signals to prevent triggering changes while repopulating
        self.channelCombo.blockSignals(True)
        current_channel_data = network_data.get('interface_channel', None)
        self.channelCombo.clear()
        self.channelCombo.addItem("- Chưa chọn -", None) # Add a null option
        if available_channels:
            for display_name, channel_data in available_channels.items():
                 # Lưu trữ data kênh (có thể là app_name, chan_index từ vector)
                 self.channelCombo.addItem(display_name, channel_data)
                 # Chọn lại kênh đã lưu nếu có
                 if channel_data == current_channel_data:
                      self.channelCombo.setCurrentText(display_name)
        # Set index nếu không tìm thấy kênh đã lưu
        if self.channelCombo.currentIndex() == -1:
              # Nếu có kênh đã lưu nhưng ko tìm thấy trong list mới, thêm tạm vào
              if isinstance(current_channel_data, dict): # Check if it looks like our channel data format
                  display_name_saved = f"{current_channel_data.get('app_name', 'Unknown')} - Channel {current_channel_data.get('chan_index', '?')} (Not Detected)"
                  self.channelCombo.addItem(display_name_saved, current_channel_data)
                  self.channelCombo.setCurrentIndex(self.channelCombo.count() - 1)
              else:
                   self.channelCombo.setCurrentIndex(0) # Default to "Chưa chọn"
        self.channelCombo.blockSignals(False)


        # --- Cập nhật các cài đặt khác ---
        # Block signals while setting values programmatically
        was_blocked_br = self.baudrateCombo.blockSignals(True)
        baud_rate = network_data.get('baud_rate', 500000) # Default 500k
        self.baudrateCombo.setCurrentText(str(baud_rate))
        self.baudrateCombo.blockSignals(was_blocked_br)

        was_blocked_fd = self.fdCheckbox.blockSignals(True)
        is_fd = network_data.get('is_fd', False)
        self.fdCheckbox.setChecked(is_fd)
        self.fdCheckbox.blockSignals(was_blocked_fd)

        # Block signals for data baud rate combo
        was_blocked_dbr = self.dataBaudrateCombo.blockSignals(True)
        data_baud_rate = network_data.get('data_baud_rate', 2000000) # Default 2M
        self.dataBaudrateCombo.setCurrentText(str(data_baud_rate))
        self.dataBaudrateCombo.setEnabled(is_fd) # Enable/disable based on FD checkbox
        self.dataBaudrateCombo.blockSignals(was_blocked_dbr)

        # --- Cập nhật trạng thái kết nối ---
        status = network_data.get('connection_status', 'offline')
        is_online = (status == 'online')
        self.update_status_display(status)

        # Enable/disable các controls dựa trên trạng thái
        self.connectButton.setEnabled(not is_online and self.channelCombo.currentData() is not None) # Enable connect only if offline and channel selected
        self.disconnectButton.setEnabled(is_online)
        self.channelCombo.setEnabled(not is_online)
        self.baudrateCombo.setEnabled(not is_online)
        self.fdCheckbox.setEnabled(not is_online)
        self.dataBaudrateCombo.setEnabled(not is_online and is_fd) # Only enable if FD is checked AND offline
        self.rescanButton.setEnabled(not is_online) # Can only rescan when offline

    def update_status_display(self, status_key):
        """Cập nhật label và màu sắc của trạng thái."""
        display_text, color = CONNECTION_STATUS.get(status_key, ("Unknown", QColor("black")))
        self.statusLabel.setText(display_text)
        self.statusLabel.setStyleSheet(f"QLabel {{ color : {color.name()}; }}")

# Tab Trace/Messages (Cập nhật để nhận live data)
class TraceMessagesTab(BaseNetworkTab): # Sửa đổi nhiều
    signalValueUpdate = pyqtSignal(str, str, object, object) # net_id, sig_name, value, timestamp_obj
    # Maximum rows to keep in the table to prevent memory issues with long live traces
    MAX_TABLE_ROWS = 10000

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)

        control_layout = QHBoxLayout()
        self.loadTraceButton = QPushButton(QIcon.fromTheme("document-open"), "Tải File Trace (CSV)...")
        self.loadTraceButton.setToolTip("Load messages from a CSV file (available when Offline)")
        self.loadTraceButton.clicked.connect(self._request_load_trace)

        self.liveStatusLabel = QLabel("Mode: Offline") # Cho biết đang xem live hay file
        self.messageCounterLabel = QLabel("Msgs: 0")

        self.clearButton = QPushButton(QIcon.fromTheme("edit-clear"), "Xóa Bảng")
        self.clearButton.clicked.connect(self._clear_table)

        control_layout.addWidget(self.loadTraceButton)
        control_layout.addWidget(self.liveStatusLabel)
        control_layout.addStretch(1)
        control_layout.addWidget(self.messageCounterLabel)
        control_layout.addWidget(self.clearButton)

        layout.addLayout(control_layout)

        self.traceTable = QTableWidget()
        self._setup_trace_table()
        layout.addWidget(self.traceTable)

        self._message_count = 0
        self._is_live_mode = False

    def _setup_trace_table(self): # Thêm các cột từ can.Message
        self.traceTable.setColumnCount(8) # Timestamp, ID, Ext, RTR, ERR, DLC, Data, Decoded
        headers = ["Timestamp", "ID (Hex)", "Xtd", "RTR", "ERR", "DLC", "Data (Hex)", "Decoded Signals"]
        self.traceTable.setHorizontalHeaderLabels(headers)
        self.traceTable.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        # Điều chỉnh kích thước cột mặc định
        self.traceTable.setColumnWidth(0, 120) # Timestamp
        self.traceTable.setColumnWidth(1, 80)  # ID
        self.traceTable.setColumnWidth(2, 30)  # Xtd
        self.traceTable.setColumnWidth(3, 30)  # RTR
        self.traceTable.setColumnWidth(4, 30)  # ERR
        self.traceTable.setColumnWidth(5, 30)  # DLC
        self.traceTable.setColumnWidth(6, 200) # Data
        self.traceTable.horizontalHeader().setStretchLastSection(True) # Decoded giãn ra
        self.traceTable.setEditTriggers(QTableWidget.NoEditTriggers)
        self.traceTable.setSelectionBehavior(QTableWidget.SelectRows)
        self.traceTable.setAlternatingRowColors(True)
        self.traceTable.setWordWrap(False)
        self.traceTable.setVerticalScrollMode(QTableWidget.ScrollPerPixel)
        self.traceTable.setHorizontalScrollMode(QTableWidget.ScrollPerPixel)

        # Tối ưu hóa cho live data: bật sorting có thể chậm
        # self.traceTable.setSortingEnabled(True)

    def _request_load_trace(self):
        if self.current_network_id and not self._is_live_mode: # Chỉ cho phép load file khi offline
            self.loadTraceRequested.emit(self.current_network_id)
        elif self._is_live_mode:
            QMessageBox.information(self, "Chế độ Live", "Không thể tải file trace khi đang kết nối trực tiếp. Vui lòng Ngắt kết nối trước.")

    def _clear_table(self):
        self.traceTable.setRowCount(0)
        self._message_count = 0
        self.messageCounterLabel.setText("Msgs: 0")
        # Nếu muốn xóa cả dữ liệu gốc (trace_data) thì cần emit signal về MainWindow


    def update_content(self, network_id, network_data, available_channels=None):
        super().update_content(network_id, network_data)
        status = network_data.get('connection_status', 'offline')
        self._is_live_mode = (status == 'online')

        if self._is_live_mode:
            # Nếu chuyển sang live, xóa bảng cũ (dữ liệu từ file)
            self.liveStatusLabel.setText("Mode: Online (Live)")
            self.loadTraceButton.setEnabled(False)
            if self.traceTable.rowCount() > 0: # Chỉ xóa nếu bảng có dữ liệu file cũ
                 # Có thể hỏi user trước khi xóa
                 # reply = QMessageBox.question(...)
                 # if reply == QMessageBox.Yes:
                 #     self._clear_table()
                 # Hoặc tự động xóa:
                 self._clear_table()
        else:
            # Nếu offline, hiển thị dữ liệu từ file (nếu có)
            self.liveStatusLabel.setText("Mode: Offline (File)")
            self.loadTraceButton.setEnabled(True)
            trace_path = network_data.get('trace_path', None)
            trace_data = network_data.get('trace_data', [])
            db = network_data.get('db', None)
            self.populate_from_file_data(trace_data, db) # Populate lại từ file data

    def populate_from_file_data(self, trace_data, db):
        """Điền dữ liệu vào bảng từ trace_data (danh sách list)."""
        self._clear_table() # Xóa bảng trước khi điền mới
        if not trace_data:
             return

        # Tối ưu: disable updates
        self.traceTable.setUpdatesEnabled(False)
        self.traceTable.setSortingEnabled(False) # Tắt sort khi đang thêm nhiều dòng

        limited_data = trace_data[-self.MAX_TABLE_ROWS:] # Chỉ hiển thị MAX_TABLE_ROWS dòng cuối
        self.traceTable.setRowCount(len(limited_data))

        for row_idx, row_file_data in enumerate(limited_data):
             timestamp, id_hex, dlc, data_hex = row_file_data[:4] # Lấy 4 phần tử đầu

             # Tạo các QTableWidgetItem
             item_ts = QTableWidgetItem(timestamp)
             item_id = QTableWidgetItem(id_hex)
             # Các cột live data (Xtd, RTR, ERR) để trống khi load từ file CSV đơn giản
             item_xtd = QTableWidgetItem("")
             item_rtr = QTableWidgetItem("")
             item_err = QTableWidgetItem("")
             item_dlc = QTableWidgetItem(dlc)
             item_data = QTableWidgetItem(data_hex)
             item_decoded = QTableWidgetItem("")

             # Căn chỉnh
             item_id.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
             item_dlc.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)

             # Decode nếu có DB
             decoded_str = self._decode_message(id_hex, data_hex, db, timestamp)
             if decoded_str:
                  item_decoded.setText(decoded_str)
                  item_decoded.setToolTip(decoded_str)

             # Set items vào bảng
             self.traceTable.setItem(row_idx, 0, item_ts)
             self.traceTable.setItem(row_idx, 1, item_id)
             self.traceTable.setItem(row_idx, 2, item_xtd)
             self.traceTable.setItem(row_idx, 3, item_rtr)
             self.traceTable.setItem(row_idx, 4, item_err)
             self.traceTable.setItem(row_idx, 5, item_dlc)
             self.traceTable.setItem(row_idx, 6, item_data)
             self.traceTable.setItem(row_idx, 7, item_decoded)
             self._message_count += 1


        self.messageCounterLabel.setText(f"Msgs: {self._message_count} (from file)")
        # self.traceTable.setSortingEnabled(True) # Bật lại sort nếu muốn
        self.traceTable.setUpdatesEnabled(True)
        self.traceTable.scrollToBottom() # Cuộn xuống cuối


    def add_live_message(self, msg: can.Message, db: cantools.db.Database = None):
        """Thêm một message trực tiếp vào bảng."""
        if not self._is_live_mode: return # Chỉ thêm khi đang online

        # Giới hạn số dòng trong bảng
        if self._message_count >= self.MAX_TABLE_ROWS:
             self.traceTable.removeRow(0) # Xóa dòng đầu tiên
        else:
             self.traceTable.setRowCount(self._message_count + 1) # Thêm dòng mới

        row_idx = self.traceTable.rowCount() - 1 # Index của dòng mới (hoặc dòng bị ghi đè)

        # Tạo items từ can.Message
        item_ts = QTableWidgetItem(f"{msg.timestamp:.6f}")
        item_id = QTableWidgetItem(f"{msg.arbitration_id:X}")
        item_xtd = QTableWidgetItem("Y" if msg.is_extended_id else "N")
        item_rtr = QTableWidgetItem("Y" if msg.is_remote_frame else "N")
        item_err = QTableWidgetItem("Y" if msg.is_error_frame else "N")
        item_dlc = QTableWidgetItem(str(msg.dlc))
        item_data = QTableWidgetItem(msg.data.hex().upper())
        item_decoded = QTableWidgetItem("")

        # Căn chỉnh
        item_id.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        item_dlc.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        item_xtd.setTextAlignment(Qt.AlignCenter)
        item_rtr.setTextAlignment(Qt.AlignCenter)
        item_err.setTextAlignment(Qt.AlignCenter)
        # if msg.is_error_frame: item_id.setForeground(QColor("red")) # Highlight error frame

        # Decode nếu có DB
        decoded_str = self._decode_message(f"{msg.arbitration_id:X}", msg.data.hex(), db, msg.timestamp)
        if decoded_str:
             item_decoded.setText(decoded_str)
             item_decoded.setToolTip(decoded_str)

        # Thêm items vào hàng mới
        # Tắt tạm update để tăng hiệu năng? Có thể không cần nếu chỉ thêm 1 dòng
        # self.traceTable.setUpdatesEnabled(False)
        self.traceTable.setItem(row_idx, 0, item_ts)
        self.traceTable.setItem(row_idx, 1, item_id)
        self.traceTable.setItem(row_idx, 2, item_xtd)
        self.traceTable.setItem(row_idx, 3, item_rtr)
        self.traceTable.setItem(row_idx, 4, item_err)
        self.traceTable.setItem(row_idx, 5, item_dlc)
        self.traceTable.setItem(row_idx, 6, item_data)
        self.traceTable.setItem(row_idx, 7, item_decoded)
        # self.traceTable.setUpdatesEnabled(True)


        self._message_count += 1
        self.messageCounterLabel.setText(f"Msgs: {self._message_count} (Live)")

        # Tự động cuộn xuống nếu đang ở gần cuối
        scrollbar = self.traceTable.verticalScrollBar()
        if scrollbar.value() >= scrollbar.maximum() - scrollbar.pageStep() :
            self.traceTable.scrollToBottom()


    def _decode_message(self, id_hex, data_hex, db, timestamp):
         """Helper function to decode a single message and emit signal."""
         decoded_signals_dict = {}
         if db and data_hex: # Cần có DB và data để giải mã
              try:
                  can_id = int(id_hex, 16)
                  message_def = db.get_message_by_frame_id(can_id)
                  data_bytes = bytes.fromhex(data_hex)
                  # Decode (bỏ qua choices, cho phép truncated)
                  decoded_signals_dict = message_def.decode(data_bytes, decode_choices=False, allow_truncated=True)

                  # Format string để hiển thị trong bảng
                  decoded_str_parts = []
                  for name, val in decoded_signals_dict.items():
                        # Format số float với độ chính xác hợp lý
                        if isinstance(val, float):
                           formatted_val = f"{val:.4g}"
                        else:
                           formatted_val = str(val)
                        decoded_str_parts.append(f"{name}={formatted_val}")
                  decoded_display_str = "; ".join(decoded_str_parts)

                  # -- Emit signal cập nhật giá trị cho các tab khác --
                  ts_obj = timestamp # Dùng timestamp gốc (float hoặc string)
                  for sig_name, sig_value in decoded_signals_dict.items():
                      # Thêm check network_id trước khi emit? BaseNetworkTab không lưu sẵn db
                      if self.current_network_id: # Cần chắc chắn current_network_id là đúng
                          self.signalValueUpdate.emit(self.current_network_id, sig_name, sig_value, ts_obj)

                  return decoded_display_str

              except KeyError:
                  return "(ID không có trong DBC)" # ID not in DBC
              except ValueError:
                   return "(Lỗi dữ liệu Hex)" # Invalid hex data
              except Exception as e:
                   # print(f"Decode Error ({id_hex}): {e}") # Ghi log lỗi chi tiết hơn
                   return f"(Lỗi Decode)"
         return "" # Trả về chuỗi rỗng nếu không decode


# Tab Signal Data (Cập nhật để nhận signalValueUpdate)
class SignalDataTab(BaseNetworkTab): # Ít thay đổi logic, chỉ nhận update
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        self.signalTable = QTableWidget()
        self._setup_signal_table()
        layout.addWidget(self.signalTable)
        self.signal_row_map = {} # Map tên signal -> chỉ số hàng để update nhanh
        # Lưu ý: latest_signal_values giờ sẽ được quản lý trong MainWindow và truyền vào qua update_content

    def _setup_signal_table(self): # Giống bản trước
        self.signalTable.setColumnCount(4)
        headers = ["Signal Name", "Current Value", "Unit", "Last Update Timestamp"]
        self.signalTable.setHorizontalHeaderLabels(headers)
        self.signalTable.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.signalTable.horizontalHeader().setStretchLastSection(False)
        self.signalTable.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.signalTable.setEditTriggers(QTableWidget.NoEditTriggers)
        self.signalTable.setSelectionBehavior(QTableWidget.SelectRows)
        self.signalTable.setAlternatingRowColors(True)

    def update_content(self, network_id, network_data, available_channels=None):
        super().update_content(network_id, network_data)
        db = network_data.get('db', None)
        latest_values = network_data.get('latest_signal_values', {}) # Lấy giá trị mới nhất
        self.populate_signal_list(db, latest_values)

    def populate_signal_list(self, db, latest_values):
        self.signalTable.setRowCount(0)
        self.signal_row_map.clear()
        if not db: return

        # Tạo danh sách tín hiệu từ DBC
        signals = sorted(
             [sig for msg in db.messages for sig in msg.signals],
             key=lambda s: s.name
        )

        self.signalTable.setUpdatesEnabled(False)
        self.signalTable.setRowCount(len(signals))

        for row_idx, sig in enumerate(signals):
            sig_name = sig.name
            self.signal_row_map[sig_name] = row_idx # Lưu map để update

            item_name = QTableWidgetItem(sig_name)
            item_value = QTableWidgetItem("-") # Giá trị ban đầu
            item_unit = QTableWidgetItem(sig.unit if sig.unit else "")
            item_ts = QTableWidgetItem("-")

            # Điền giá trị mới nhất nếu có
            if sig_name in latest_values:
                value, timestamp = latest_values[sig_name]
                value_str = f"{value:.4g}" if isinstance(value, (float, int)) else str(value)
                ts_str = f"{timestamp:.6f}" if isinstance(timestamp, float) else str(timestamp)
                item_value.setText(value_str)
                item_ts.setText(ts_str)

            self.signalTable.setItem(row_idx, 0, item_name)
            self.signalTable.setItem(row_idx, 1, item_value)
            self.signalTable.setItem(row_idx, 2, item_unit)
            self.signalTable.setItem(row_idx, 3, item_ts)

        self.signalTable.setUpdatesEnabled(True)

    def update_signal_value(self, signal_name, value, timestamp):
        """Cập nhật giá trị của một signal trong bảng (được gọi từ MainWindow)."""
        if signal_name in self.signal_row_map:
            row = self.signal_row_map[signal_name]
            value_str = f"{value:.4g}" if isinstance(value, (float, int)) else str(value)
            ts_str = f"{timestamp:.6f}" if isinstance(timestamp, float) else str(timestamp)
            # Cập nhật trực tiếp các cell
            self.signalTable.item(row, 1).setText(value_str)
            self.signalTable.item(row, 3).setText(ts_str)
        # else: signal might not be in the current DBC view

# Tab Graphing (Cập nhật để nhận dữ liệu live/file)
class GraphingTab(BaseNetworkTab): # Sửa đổi để nhận data timeseries
    MAX_PLOT_POINTS = 10000 # Giới hạn số điểm vẽ để tránh lag

    def __init__(self, parent=None):
         super().__init__(parent)
         layout = QVBoxLayout(self)
         layout.setContentsMargins(5,5,5,5)

         if PYQTGRAPH_AVAILABLE:
              graph_splitter = QSplitter(Qt.Horizontal)
              # Control panel (signal selection)
              control_widget = QWidget()
              control_layout = QVBoxLayout(control_widget)
              control_layout.addWidget(QLabel("Chọn Tín hiệu:"))
              self.signalListWidget = QListWidget()
              self.signalListWidget.setSelectionMode(QListWidget.ExtendedSelection)
              self.signalListWidget.itemSelectionChanged.connect(self.plot_selected_signals)
              control_layout.addWidget(self.signalListWidget)
              clear_button = QPushButton(QIcon.fromTheme("edit-clear"), "Xóa Đồ thị")
              clear_button.clicked.connect(self._clear_plots)
              control_layout.addWidget(clear_button)
              control_widget.setMaximumWidth(300)

              # Plot area
              self.plotWidget = pg.PlotWidget()
              self.plotWidget.setBackground('w')
              self.plotWidget.showGrid(x=True, y=True)
              self.plot_items = {} # {sig_name: plot_data_item}

              graph_splitter.addWidget(control_widget)
              graph_splitter.addWidget(self.plotWidget)
              graph_splitter.setSizes([250, 650])
              layout.addWidget(graph_splitter)
         else:
              layout.addWidget(QLabel("pyqtgraph chưa cài đặt."))

         self._current_timeseries_data = {} # Dữ liệu được truyền từ MainWindow

    def _clear_plots(self):
        if PYQTGRAPH_AVAILABLE:
             self.plotWidget.clear()
             self.plot_items = {}
             # Bỏ chọn trong list
             self.signalListWidget.clearSelection()
             # Có thể thêm legend lại nếu cần
             self.plotWidget.addLegend(offset=(-30, 30))


    def update_content(self, network_id, network_data, available_channels=None):
        super().update_content(network_id, network_data)
        # Lấy dữ liệu timeseries đã xử lý (từ file hoặc live tích lũy)
        self._current_timeseries_data = network_data.get('signal_time_series', {})
        db = network_data.get('db', None)

        if PYQTGRAPH_AVAILABLE:
            # Cập nhật danh sách tín hiệu có sẵn để vẽ
            self.signalListWidget.clear()
            if db:
                signals_with_data = sorted([
                    sig.name for msg in db.messages for sig in msg.signals
                    if sig.name in self._current_timeseries_data and self._current_timeseries_data[sig.name][0] # Check có timestamp
                ])
                self.signalListWidget.addItems(signals_with_data)

            # Vẽ lại các tín hiệu đang được chọn (nếu có)
            self.plot_selected_signals()


    def plot_selected_signals(self):
         if not PYQTGRAPH_AVAILABLE: return
         selected_names = {item.text() for item in self.signalListWidget.selectedItems()}
         existing_plots = set(self.plot_items.keys())

         # Remove plots not selected anymore
         plots_to_remove = existing_plots - selected_names
         for name in plots_to_remove:
              if name in self.plot_items:
                   self.plotWidget.removeItem(self.plot_items[name])
                   del self.plot_items[name]

         # Add new plots or update existing ones
         pens = [pg.mkPen(color=c, width=1) for c in ['b', 'r', 'g', 'c', 'm', 'y', 'k'] * 5]
         plot_index = 0
         needs_legend = False

         for name in selected_names:
              if name not in self.plot_items: # Only add if not already plotted
                   if name in self._current_timeseries_data:
                        timestamps, values = self._current_timeseries_data[name]
                        if timestamps and values and len(timestamps) == len(values):
                              # Lọc chỉ lấy giá trị số
                              numeric_ts = []
                              numeric_vals = []
                              for t, v in zip(timestamps, values):
                                   if isinstance(v, (int, float)):
                                        try: # Ensure timestamp is numeric
                                             numeric_ts.append(float(t))
                                             numeric_vals.append(float(v))
                                        except (ValueError, TypeError):
                                             continue # Skip non-numeric timestamps

                              if numeric_ts and numeric_vals:
                                   # Giới hạn số điểm vẽ
                                   if len(numeric_ts) > self.MAX_PLOT_POINTS:
                                        indices = [int(i * (len(numeric_ts)-1) / (self.MAX_PLOT_POINTS-1)) for i in range(self.MAX_PLOT_POINTS)]
                                        sampled_ts = [numeric_ts[i] for i in indices]
                                        sampled_vals = [numeric_vals[i] for i in indices]
                                        plot_item = self.plotWidget.plot(sampled_ts, sampled_vals, pen=pens[plot_index % len(pens)], name=name)
                                        # Có thể thêm label "(sampled)" vào name nếu muốn
                                   else:
                                        plot_item = self.plotWidget.plot(numeric_ts, numeric_vals, pen=pens[plot_index % len(pens)], name=name)

                                   self.plot_items[name] = plot_item
                                   plot_index += 1
                                   needs_legend = True
                        # else: print(f"Graph: No valid data for {name}")
                   # else: print(f"Graph: No timeseries data found for {name}")

         # Clear and add legend only if needed
         if needs_legend or plots_to_remove: # Add/Update legend if plots changed
            # Check if legend exists, if so remove it? pyqtgraph might handle duplicates.
             self.plotWidget.addLegend(offset=(-30, 30))

    def update_plot_data(self, signal_name, timestamp, value):
         """Appends a new data point to an existing plot (if plotted)."""
         if not PYQTGRAPH_AVAILABLE or signal_name not in self.plot_items:
             return

         plot_item = self.plot_items[signal_name]
         try:
             # Lấy dữ liệu hiện có từ plot item
             x_data, y_data = plot_item.getData()
             if x_data is None: # Plot might have been cleared or empty
                 x_data = []
                 y_data = []
             # Chuyển đổi sang list nếu là numpy array
             x_data = list(x_data)
             y_data = list(y_data)

             # Thêm điểm mới (đảm bảo là float)
             x_data.append(float(timestamp))
             y_data.append(float(value))

             # Giới hạn số điểm
             if len(x_data) > self.MAX_PLOT_POINTS:
                 x_data = x_data[-self.MAX_PLOT_POINTS:]
                 y_data = y_data[-self.MAX_PLOT_POINTS:]

             # Cập nhật lại plot item
             plot_item.setData(x_data, y_data)

         except (ValueError, TypeError):
             # Ignore if timestamp/value cannot be converted to float
             pass
         except Exception as e:
             print(f"Error updating plot for {signal_name}: {e}")

# Tab Logging (Cập nhật để biết trạng thái Online/Offline)
class LoggingTab(BaseNetworkTab): # Ít thay đổi logic, chỉ thay đổi label/tooltip
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setAlignment(Qt.AlignTop)

        self.statusLabel = QLabel("Trạng thái Log: Đang dừng")
        layout.addWidget(self.statusLabel)

        file_layout = QHBoxLayout()
        self.logPathEdit = QLineEdit()
        self.logPathEdit.setReadOnly(True)
        select_button = QPushButton("Chọn File Log...")
        select_button.clicked.connect(self._request_select_log_file)
        file_layout.addWidget(QLabel("File Log:"))
        file_layout.addWidget(self.logPathEdit, 1)
        file_layout.addWidget(select_button)
        layout.addLayout(file_layout)

        self.toggleLogButton = QPushButton(QIcon.fromTheme("media-record"), "Bắt đầu Ghi")
        self.toggleLogButton.setCheckable(True)
        self.toggleLogButton.toggled.connect(self._handle_toggle_log)
        layout.addWidget(self.toggleLogButton)

        self.logCountLabel = QLabel("Messages Logged: 0")
        layout.addWidget(self.logCountLabel)


        layout.addStretch(1)
        self._is_live_mode = False

    def _request_select_log_file(self):
        if self.current_network_id:
            self.selectLogFileRequested.emit(self.current_network_id)

    def _handle_toggle_log(self, checked):
        if self.current_network_id:
            if checked and not self.network_data.get('log_path'):
                QMessageBox.warning(self, "Thiếu File Log", "Vui lòng chọn file log trước.")
                self.toggleLogButton.setChecked(False)
                return
            # Logic ghi log giờ sẽ hoạt động với live data nếu online
            self.toggleLoggingRequested.emit(self.current_network_id)

    def update_content(self, network_id, network_data, available_channels=None):
        super().update_content(network_id, network_data)
        is_logging = network_data.get('is_logging', False)
        log_path = network_data.get('log_path', None)
        status = network_data.get('connection_status', 'offline')
        self._is_live_mode = (status == 'online')
        log_count = network_data.get('log_message_count', 0)

        self.logPathEdit.setText(log_path if log_path else "")
        self.logCountLabel.setText(f"Messages Logged: {log_count}")

        was_blocked = self.toggleLogButton.blockSignals(True)
        self.toggleLogButton.setChecked(is_logging)
        log_mode = "(Live Data)" if self._is_live_mode else "(Offline Data - Requires Load)"

        if is_logging:
            self.statusLabel.setText(f"Trạng thái Log: Đang ghi {log_mode}")
            self.toggleLogButton.setText("Dừng Ghi")
            self.toggleLogButton.setIcon(QIcon.fromTheme("media-playback-stop"))
            # Nút Chọn File có thể disable khi đang ghi?
            # self.logPathEdit.parent().findChild(QPushButton).setEnabled(False)
        else:
            self.statusLabel.setText(f"Trạng thái Log: Đang dừng {log_mode}")
            self.toggleLogButton.setText("Bắt đầu Ghi")
            self.toggleLogButton.setIcon(QIcon.fromTheme("media-record"))
            # self.logPathEdit.parent().findChild(QPushButton).setEnabled(True)
        self.toggleLogButton.blockSignals(was_blocked)

        # Cập nhật tooltip cho nút Start/Stop
        if self._is_live_mode:
            self.toggleLogButton.setToolTip("Bắt đầu/Dừng ghi dữ liệu trực tiếp từ phần cứng.")
        else:
            self.toggleLogButton.setToolTip("Bắt đầu/Dừng ghi dữ liệu từ file trace đã tải (nếu có).")

    def set_log_count(self, count):
        self.logCountLabel.setText(f"Messages Logged: {count}")

# --- Cửa sổ Chính (Sửa đổi nhiều) ---
class MultiCanManagerApp(QMainWindow):
    # Signal to update tabs AFTER main data is modified
    networkDataUpdated = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.networks_data = {}
        self.workers = {} # Lưu các workers đang hoạt động (DBC, Trace, Logging, Listener)
        self.next_network_id_counter = 1
        self.current_selected_network_id = None
        self.available_vector_channels = {} # { display_name: channel_data }
        self.initUI()
        if PYTHON_CAN_AVAILABLE:
            # Quét kênh lần đầu khi khởi động (có thể làm trong thread nền)
            QTimer.singleShot(100, self.scan_vector_channels) # Delay nhẹ để UI hiện lên
        else:
             self.statusBar.showMessage("Lỗi: python-can không khả dụng. Không thể kết nối phần cứng.", 10000)


    def initUI(self):
        self.setWindowTitle("Multi-Network CAN Manager (Vector Hardware)")
        self.setGeometry(100, 100, 1600, 950)

        self.setup_menu()

        self.splitter = QSplitter(Qt.Horizontal)

        # Panel Trái: Cây Mạng (Giữ nguyên)
        self.networkTreeWidget = QTreeWidget()
        self.setup_network_tree()
        tree_container = QWidget()
        tree_layout = QVBoxLayout(tree_container)
        tree_layout.setContentsMargins(0,0,0,0)
        tree_layout.addWidget(self.networkTreeWidget)
        tree_container.setMaximumWidth(350)
        self.splitter.addWidget(tree_container)

        # Panel Phải: Tabs
        self.detailsTabWidget = QTabWidget()
        self.setup_detail_tabs() # Tạo các tab widget
        self.splitter.addWidget(self.detailsTabWidget)

        self.splitter.setSizes([300, 1300])
        self.splitter.setChildrenCollapsible(False)
        self.setCentralWidget(self.splitter)

        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusLabel = QLabel("Sẵn sàng")
        self.statusBar.addWidget(self.statusLabel, 1)

        # Connect the data updated signal to the UI update slot
        self.networkDataUpdated.connect(self.update_details_for_current_network)

        self.show()

    def setup_menu(self): # Giữ nguyên từ bản trước
        menu_bar = self.menuBar()
        # File Menu
        file_menu = menu_bar.addMenu("&File")
        exit_action = QAction(QIcon.fromTheme("application-exit"), "&Thoát", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        # Network Menu
        network_menu = menu_bar.addMenu("&Network")
        add_net_action = QAction(QIcon.fromTheme("network-wired"), "&Thêm Mạng CAN Mới", self)
        add_net_action.triggered.connect(self.add_new_network)
        network_menu.addAction(add_net_action)
        # Thêm Action quét kênh ở đây hoặc trong tab Hardware
        scan_channels_action = QAction(QIcon.fromTheme("view-refresh"), "&Quét lại Kênh Vector", self)
        scan_channels_action.triggered.connect(self.scan_vector_channels)
        network_menu.addAction(scan_channels_action)
        # Help Menu
        help_menu = menu_bar.addMenu("&Help")
        about_action = QAction("&Giới thiệu", self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)


    def setup_network_tree(self): # Giữ nguyên
        self.networkTreeWidget.setHeaderHidden(True)
        root_item = QTreeWidgetItem(self.networkTreeWidget, ["CAN Networks"])
        self.networkTreeWidget.addTopLevelItem(root_item)
        root_item.setExpanded(True)
        root_item.setFlags(root_item.flags() & ~Qt.ItemIsEditable)
        self.networkTreeWidget.itemSelectionChanged.connect(self.on_network_selection_changed)
        self.networkTreeWidget.itemChanged.connect(self.on_network_item_changed)

    def setup_detail_tabs(self):
        self.hwConfigTab = HardwareConfigTab() # Tab mới
        self.dbcTab = DbcConfigTab()
        self.traceTab = TraceMessagesTab()
        self.signalsTab = SignalDataTab()
        self.graphTab = GraphingTab()
        self.logTab = LoggingTab()

        # Thứ tự Tab hợp lý hơn
        self.detailsTabWidget.addTab(self.hwConfigTab, QIcon.fromTheme("preferences-system"), "Hardware Config")
        self.detailsTabWidget.addTab(self.dbcTab, QIcon.fromTheme("database"), "DBC Config")
        self.detailsTabWidget.addTab(self.traceTab, QIcon.fromTheme("text-x-generic"), "Trace / Messages")
        self.detailsTabWidget.addTab(self.signalsTab, QIcon.fromTheme("view-list-details"), "Signal Data")
        self.detailsTabWidget.addTab(self.graphTab, QIcon.fromTheme("utilities-system-monitor"), "Graphing")
        self.detailsTabWidget.addTab(self.logTab, QIcon.fromTheme("document-save"), "Logging")

        # Kết nối Signals từ các Tab đến MainWindow
        self.hwConfigTab.connectRequested.connect(self.connect_network)
        self.hwConfigTab.disconnectRequested.connect(self.disconnect_network)
        self.hwConfigTab.rescanChannelsRequested.connect(self.scan_vector_channels)
        self.hwConfigTab.configChanged.connect(self.handle_hw_config_change) # NEW: Handle hw config update
        self.dbcTab.loadDbcRequested.connect(self.handle_load_dbc)
        self.traceTab.loadTraceRequested.connect(self.handle_load_trace)
        # Tín hiệu cập nhật signal value giờ sẽ được xử lý trong handle_live_message
        # self.traceTab.signalValueUpdate.connect(self.handle_signal_value_update) # Remove this connection
        self.logTab.selectLogFileRequested.connect(self.handle_select_log_file)
        self.logTab.toggleLoggingRequested.connect(self.handle_toggle_logging)

        self.detailsTabWidget.setEnabled(False)


    # --- Quản lý Mạng ---
    def add_new_network(self): # Thêm các trường mới cho hardware
        network_id = str(uuid.uuid4())
        network_name = f"CAN Network {self.next_network_id_counter}"
        self.next_network_id_counter += 1

        self.networks_data[network_id] = {
            "id": network_id, # Store ID also inside for convenience
            "name": network_name,
            "dbc_path": None, "db": None,
            "trace_path": None, "trace_data": [],
            "signal_time_series": {}, "latest_signal_values": {},
            "log_path": None, "is_logging": False, "logging_worker": None, "log_message_count": 0,
            # --- Hardware Fields ---
            "interface_channel": None, # Dữ liệu kênh đã chọn (từ detect_available_configs)
            "baud_rate": 500000,     # Default baud rate
            "is_fd": False,
            "data_baud_rate": 2000000,# Default FD data rate
            "connection_status": "offline", # "offline", "online", "connecting", "error"
            "can_bus": None,          # Đối tượng can.Bus khi kết nối
            "listener_thread": None, # Luồng nhận message khi kết nối
            "last_hw_error": None     # Lưu lỗi phần cứng gần nhất
        }
        # ... (thêm vào cây và chọn item như trước) ...
        root = self.networkTreeWidget.topLevelItem(0)
        network_item = QTreeWidgetItem(root, [network_name])
        network_item.setData(0, Qt.UserRole, network_id)
        network_item.setFlags(network_item.flags() | Qt.ItemIsEditable)
        root.addChild(network_item)
        self.networkTreeWidget.setCurrentItem(network_item) # Trigger selection change
        self.statusLabel.setText(f"Added new network: {network_name}")

    def on_network_selection_changed(self):
         selected_items = self.networkTreeWidget.selectedItems()
         new_network_id = None
         if selected_items:
              selected_item = selected_items[0]
              item_data = selected_item.data(0, Qt.UserRole)
              if item_data and isinstance(item_data, str): # Check it's a network item
                  new_network_id = item_data

         if new_network_id != self.current_selected_network_id:
             self.current_selected_network_id = new_network_id
             if new_network_id:
                 self.detailsTabWidget.setEnabled(True)
                 self.update_details_for_current_network() # Central update point
             else:
                 self.detailsTabWidget.setEnabled(False) # Disable tabs if root or invalid is selected
                 self.statusLabel.setText("Please select a CAN network.")

    def update_details_for_current_network(self, network_id=None):
        """Updates all detail tabs based on the currently selected network's data."""
        target_id = network_id or self.current_selected_network_id
        if not target_id or target_id not in self.networks_data:
            self.detailsTabWidget.setEnabled(False)
            return

        self.detailsTabWidget.setEnabled(True)
        network_data = self.networks_data[target_id]
        net_name = network_data.get('name', 'N/A')
        self.statusLabel.setText(f"Viewing Network: {net_name} [{network_data.get('connection_status', 'offline')}]")

        # Pass data and available channels to *all* tabs
        for i in range(self.detailsTabWidget.count()):
            tab = self.detailsTabWidget.widget(i)
            if isinstance(tab, BaseNetworkTab):
                 try: # Wrap update in try-except for robustness
                      tab.update_content(target_id, network_data, self.available_vector_channels)
                 except Exception as e:
                     print(f"Error updating tab {tab.__class__.__name__} for network {target_id}: {e}")
                     traceback.print_exc()


    def on_network_item_changed(self, item, column): # Giữ nguyên
        if column == 0:
            network_id = item.data(0, Qt.UserRole)
            if network_id and network_id in self.networks_data:
                new_name = item.text(0)
                old_name = self.networks_data[network_id]['name']
                if new_name != old_name:
                    self.networks_data[network_id]['name'] = new_name
                    if network_id == self.current_selected_network_id:
                         self.statusLabel.setText(f"Viewing Network: {new_name} [{self.networks_data[network_id].get('connection_status', 'offline')}]")

    # --- Hardware Connection Logic ---
    def scan_vector_channels(self):
        """Scans for available Vector channels using python-can."""
        if not PYTHON_CAN_AVAILABLE:
            self.show_error_message("Lỗi", "Thư viện python-can không khả dụng.")
            return

        self.statusLabel.setText("Đang quét kênh Vector...")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        progress = QProgressDialog("Đang quét kênh Vector...", "Hủy", 0, 0, self)
        progress.setWindowModality(Qt.WindowModal)
        progress.show()
        QApplication.processEvents() # Ensure progress dialog is shown

        try:
            # detect_available_configs can take time
            configs = can.detect_available_configs(interface='vector')
            self.available_vector_channels = {}
            for cfg in configs:
                 # Example structure: {'interface': 'vector', 'channel': 0, 'supports_fd': True,
                 #                    'serial_number': 12345, 'hw_type': 'VN1630', ...}
                 # Tạo một display name và lưu lại cấu hình quan trọng
                 app_name = cfg.get('app_name', cfg.get('hw_type', 'Unknown HW')) # Lấy app_name nếu có (XL Driver Library >= 19)
                 chan_index = cfg.get('channel')
                 # Correct Vector channel indexing: Starts from 0 in API, display as 1-based.
                 display_name = f"{app_name} - CAN {chan_index + 1}"
                 # Lưu channel index và app_name (quan trọng để tạo Bus)
                 self.available_vector_channels[display_name] = {'app_name': app_name, 'chan_index': chan_index }
                 # print(f"Detected: {display_name} -> {self.available_vector_channels[display_name]}") # Debug

            self.statusLabel.setText(f"Đã tìm thấy {len(self.available_vector_channels)} kênh Vector.")
            # Cập nhật lại combobox trong tab hardware config nếu đang hiển thị
            if self.current_selected_network_id:
                self.networkDataUpdated.emit(self.current_selected_network_id)

        except ImportError as e: # vector interface not found by python-can?
             self.show_error_message("Lỗi Quét Kênh", f"Không thể tìm thấy giao diện Vector. Đảm bảo Vector Driver đã được cài đặt và python-can có thể thấy nó.\nLỗi: {e}")
             self.statusLabel.setText("Lỗi quét kênh: Không tìm thấy giao diện Vector.")
             self.available_vector_channels = {} # Clear list on error
        except can.CanError as e:
            self.show_error_message("Lỗi Quét Kênh", f"Lỗi từ thư viện CAN khi quét kênh:\n{e}")
            self.statusLabel.setText("Lỗi quét kênh.")
            self.available_vector_channels = {}
        except Exception as e: # Catch other potential errors
            self.show_error_message("Lỗi Không Mong Đợi", f"Lỗi không xác định khi quét kênh:\n{e}")
            self.statusLabel.setText("Lỗi không xác định khi quét kênh.")
            self.available_vector_channels = {}
        finally:
            progress.close()
            QApplication.restoreOverrideCursor()


    def handle_hw_config_change(self, network_id, key, value):
        """Update the network data when hardware config changes in the tab."""
        if network_id and network_id in self.networks_data:
            if self.networks_data[network_id].get(key) != value:
                # print(f"HW Config Changed: Net={network_id}, Key={key}, Value={value}")
                self.networks_data[network_id][key] = value
                # Có thể trigger validation hoặc cập nhật logic khác ở đây
                # Cập nhật lại trạng thái nút Connect dựa trên channel mới
                if key == 'interface_channel':
                    if self.current_selected_network_id == network_id:
                         # Directly update the button state in the active tab
                         is_offline = self.networks_data[network_id].get('connection_status', 'offline') == 'offline'
                         channel_selected = value is not None
                         self.hwConfigTab.connectButton.setEnabled(is_offline and channel_selected)


    def connect_network(self, network_id):
        """Attempts to connect the specified network to its configured hardware."""
        if not PYTHON_CAN_AVAILABLE: return
        if not network_id or network_id not in self.networks_data: return

        net_data = self.networks_data[network_id]
        if net_data['connection_status'] == 'online' or net_data['connection_status'] == 'connecting':
            print(f"Network {network_id} is already {net_data['connection_status']}.")
            return

        # Lấy cấu hình từ net_data
        channel_cfg = net_data.get('interface_channel') # This is {'app_name': ..., 'chan_index': ...}
        baud_rate = net_data.get('baud_rate')
        is_fd = net_data.get('is_fd', False)
        data_baud_rate = net_data.get('data_baud_rate') if is_fd else None

        if not channel_cfg:
            self.show_network_error(network_id, "Chưa chọn kênh Vector.")
            return
        if not baud_rate:
            self.show_network_error(network_id, "Chưa cấu hình Baud Rate.")
            return
        if is_fd and not data_baud_rate:
             self.show_network_error(network_id, "Chưa cấu hình Data Baud Rate cho CAN FD.")
             return

        app_name = channel_cfg['app_name']
        chan_index = channel_cfg['chan_index']

        net_data['connection_status'] = 'connecting'
        net_data['last_hw_error'] = None
        self.networkDataUpdated.emit(network_id) # Update UI to "Connecting..."
        QApplication.processEvents() # Force UI update

        try:
            # Tạo đối tượng Bus
            print(f"Connecting to Vector: app_name='{app_name}', channel={chan_index}, bitrate={baud_rate}, fd={is_fd}, data_bitrate={data_baud_rate}")
            can_bus = can.interface.Bus(
                interface='vector',
                app_name=app_name, # Use application name (e.g., 'VN1630')
                channel=chan_index, # API uses 0-based index
                bitrate=int(baud_rate),
                fd=is_fd,
                data_bitrate=int(data_baud_rate) if data_baud_rate else None
                # Có thể thêm các tham số khác như sjw, sample_point nếu cần
            )
            net_data['can_bus'] = can_bus
            net_data['connection_status'] = 'online'
            print(f"Network {network_id} connected successfully.")

            # Khởi tạo và bắt đầu luồng Listener
            listener_thread = CanListenerThread(network_id, can_bus)
            listener_thread.message_received.connect(self.handle_live_message)
            listener_thread.listener_error.connect(self.handle_listener_error)
            listener_thread.connection_closed.connect(self.handle_connection_closed) # Khi thread tự dừng
            net_data['listener_thread'] = listener_thread
            worker_id = f"{network_id}_listener"
            self.workers[worker_id] = listener_thread # Track the worker
            listener_thread.start()

        except VectorError as e:
             print(f"VectorError connecting {network_id}: {e}")
             net_data['connection_status'] = 'error'
             net_data['last_hw_error'] = str(e)
             self.show_network_error(network_id, f"Lỗi kết nối Vector:\n{e}\n\nKiểm tra driver, phần cứng và cấu hình.")
             net_data['can_bus'] = None
        except can.CanError as e:
            print(f"CanError connecting {network_id}: {e}")
            net_data['connection_status'] = 'error'
            net_data['last_hw_error'] = str(e)
            self.show_network_error(network_id, f"Lỗi thư viện CAN:\n{e}")
            net_data['can_bus'] = None
        except Exception as e:
             print(f"Unexpected error connecting {network_id}: {e}")
             net_data['connection_status'] = 'error'
             net_data['last_hw_error'] = f"Unexpected error: {e}"
             self.show_network_error(network_id, f"Lỗi không mong đợi khi kết nối:\n{e}")
             net_data['can_bus'] = None
        finally:
             # Luôn cập nhật UI cuối cùng
             self.networkDataUpdated.emit(network_id)


    def disconnect_network(self, network_id):
        """Disconnects the specified network from its hardware."""
        if not network_id or network_id not in self.networks_data: return

        net_data = self.networks_data[network_id]
        status = net_data.get('connection_status', 'offline')
        net_name = net_data.get('name','?')

        if status == 'offline' or status == 'error':
            print(f"Network {net_name} ({network_id}) is already {status}.")
            return # Nothing to do

        # 1. Stop the listener thread first
        listener = net_data.get('listener_thread')
        worker_id = f"{network_id}_listener"
        if listener and listener.isRunning():
            print(f"Requesting listener thread stop for {net_name}...")
            listener.stop()
            # Don't wait here, let the thread signal when it's done (handle_connection_closed)
            # if not listener.wait(1000): # Wait max 1 sec
            #      print(f"Warning: Listener thread for {net_name} did not stop gracefully.")
            # else:
            #      print(f"Listener thread for {net_name} stopped.")

        else:
             # If no listener thread running, proceed to shutdown bus directly
             print(f"No active listener found for {net_name}, proceeding with disconnect.")
             self._finalize_disconnect(network_id) # Call cleanup directly

        # Dọn dẹp worker khỏi dictionary nếu listener đã tồn tại nhưng không chạy
        if worker_id in self.workers and (listener is None or not listener.isRunning()):
             del self.workers[worker_id]


    def handle_connection_closed(self, network_id):
        """Slot called when the listener thread confirms it has stopped."""
        print(f"Listener thread for network {network_id} confirmed closed.")
        worker_id = f"{network_id}_listener"
        if worker_id in self.workers:
             del self.workers[worker_id] # Remove worker after it stopped

        # Now finalize the disconnect (shutdown bus, update status)
        self._finalize_disconnect(network_id)

    def _finalize_disconnect(self, network_id):
         """Internal: Shuts down the CAN bus and updates network state."""
         if network_id not in self.networks_data: return
         net_data = self.networks_data[network_id]
         net_name = net_data.get('name', '?')

         can_bus = net_data.get('can_bus')
         if can_bus:
              print(f"Shutting down CAN bus for {net_name}...")
              try:
                  can_bus.shutdown()
              except Exception as e:
                  print(f"Error during bus shutdown for {net_name}: {e}")
         else:
              print(f"No active bus object found to shutdown for {net_name}.")

         # Clear hardware-related data and update status
         net_data['can_bus'] = None
         net_data['listener_thread'] = None
         net_data['connection_status'] = 'offline'
         net_data['last_hw_error'] = None
         # Maybe clear live data buffers?
         # net_data['latest_signal_values'] = {}
         # net_data['signal_time_series'] = {}
         print(f"Network {net_name} finalized disconnect.")

         # Update UI
         self.networkDataUpdated.emit(network_id)


    def handle_listener_error(self, network_id, error_message):
        """Handles errors reported by the listener thread."""
        print(f"Listener Error (Net ID: {network_id}): {error_message}")
        if network_id in self.networks_data:
             self.networks_data[network_id]['connection_status'] = 'error'
             self.networks_data[network_id]['last_hw_error'] = error_message
             # The thread should stop itself, handle_connection_closed will finalize
             self.networkDataUpdated.emit(network_id) # Update UI to show error state
             self.show_network_error(network_id, f"Lỗi Listener:\n{error_message}")

    # --- Xử lý Dữ liệu Live Message ---
    def handle_live_message(self, network_id, msg: can.Message):
        """Handles a single live message received from a listener thread."""
        # This runs in the main GUI thread
        if network_id not in self.networks_data: return

        net_data = self.networks_data[network_id]
        db = net_data.get('db') # Get the DBC for this network

        # --- 1. Gửi message cho Logger (nếu đang ghi log) ---
        if net_data.get('is_logging'):
            log_worker = net_data.get('logging_worker')
            if log_worker and log_worker.isRunning():
                 log_worker.add_message(msg) # Logging worker handles formatting

        # --- 2. Decode Message và Cập nhật Dữ liệu Tín hiệu ---
        decoded_signals = {}
        if db and not msg.is_error_frame and not msg.is_remote_frame and msg.data:
            try:
                 message_def = db.get_message_by_frame_id(msg.arbitration_id)
                 # Sử dụng try-except vì decode có thể fail (DLC không khớp, data lỗi...)
                 try:
                      decoded_signals = message_def.decode(msg.data, decode_choices=False, allow_truncated=True)
                 except ValueError as ve: # E.g., wrong data length after filtering allowed truncated
                      # print(f"Decode ValueError Net {network_id} ID {msg.arbitration_id:X}: {ve} - Data: {msg.data.hex()}")
                      pass # Ignore decode error for this message? Or log it.
                 except Exception as de:
                      # print(f"Decode Error Net {network_id} ID {msg.arbitration_id:X}: {de}")
                      pass # Ignore other decode errors

                 if decoded_signals:
                      # Cập nhật latest_signal_values trong data chính
                      current_latest = net_data.get('latest_signal_values', {})
                      needs_signal_tab_update = False
                      for sig_name, sig_value in decoded_signals.items():
                            current_latest[sig_name] = (sig_value, msg.timestamp)
                            # Nếu tab signal đang hiển thị network này, update trực tiếp
                            if network_id == self.current_selected_network_id:
                                 self.signalsTab.update_signal_value(sig_name, sig_value, msg.timestamp)
                                 # Cập nhật đồ thị nếu đang vẽ tín hiệu này
                                 self.graphTab.update_plot_data(sig_name, msg.timestamp, sig_value)


            except KeyError:
                 pass # ID not in DBC


        # --- 3. Cập nhật dữ liệu timeseries (cho đồ thị) ---
        # Append to existing timeseries data structure
        current_timeseries = net_data.get('signal_time_series', {})
        for sig_name, sig_value in decoded_signals.items():
            if sig_name not in current_timeseries:
                 current_timeseries[sig_name] = ([], []) # Init (timestamps, values)
            # Append (handle potential non-numeric timestamp from earlier logic if needed)
            try:
                ts_float = float(msg.timestamp)
                val_float = float(sig_value) # Graph needs numeric values
                # Append data
                current_timeseries[sig_name][0].append(ts_float)
                current_timeseries[sig_name][1].append(val_float)
                # Limit timeseries length to avoid unbounded memory growth
                max_len = 2 * self.graphTab.MAX_PLOT_POINTS # Keep more history than plotting shows
                if len(current_timeseries[sig_name][0]) > max_len:
                     current_timeseries[sig_name] = (current_timeseries[sig_name][0][-max_len:],
                                                     current_timeseries[sig_name][1][-max_len:])
            except (ValueError, TypeError):
                 continue # Skip if value/timestamp not numeric

        # --- 4. Cập nhật Bảng Trace (nếu đang hiển thị) ---
        # Chỉ cập nhật bảng nếu tab đó đang được hiển thị VÀ network này đang được chọn
        # This can be a bottleneck if message rate is very high!
        if network_id == self.current_selected_network_id and self.detailsTabWidget.currentWidget() == self.traceTab:
             self.traceTab.add_live_message(msg, db)


    # --- Xử lý tải file và các handlers khác (Giữ nguyên hoặc cập nhật nhỏ) ---

    def handle_load_dbc(self, network_id): # Giống bản trước, dùng DbcLoadingWorker
        if not network_id or network_id not in self.networks_data: return
        worker_id = f"{network_id}_dbc"
        # ... (rest of the code similar to previous version) ...
        if worker_id in self.workers and self.workers[worker_id].isRunning(): return
        current_path = self.networks_data[network_id].get('dbc_path', None)
        dir_path = os.path.dirname(current_path) if current_path else ""
        file_path, _ = QFileDialog.getOpenFileName(self, f"Select DBC for {self.networks_data[network_id]['name']}", dir_path, "*.dbc")
        if file_path:
             self.statusLabel.setText(f"Net {self.networks_data[network_id]['name']}: Starting DBC load...")
             worker = DbcLoadingWorker(network_id, file_path)
             worker.finished.connect(self.on_dbc_loaded)
             worker.progress.connect(self.update_network_status)
             self.workers[worker_id] = worker
             worker.start()

    def handle_load_trace(self, network_id): # Giống bản trước, dùng TraceLoadingWorker
        if not network_id or network_id not in self.networks_data: return
        if self.networks_data[network_id].get('connection_status') == 'online':
             QMessageBox.information(self,"Live Mode", "Cannot load trace file while connected to hardware.")
             return
        worker_id = f"{network_id}_trace"
        # ... (rest of the code similar to previous version) ...
        if worker_id in self.workers and self.workers[worker_id].isRunning(): return
        current_path = self.networks_data[network_id].get('trace_path', None)
        dir_path = os.path.dirname(current_path) if current_path else ""
        file_path, _ = QFileDialog.getOpenFileName(self, f"Select Trace CSV for {self.networks_data[network_id]['name']}", dir_path, "*.csv")
        if file_path:
             self.statusLabel.setText(f"Net {self.networks_data[network_id]['name']}: Starting Trace load...")
             db_obj = self.networks_data[network_id].get('db')
             worker = TraceLoadingWorker(network_id, file_path, db_obj)
             worker.finished.connect(self.on_trace_loaded)
             worker.progress.connect(self.update_network_status)
             worker.progress_percent.connect(self.update_network_progress_percent)
             self.workers[worker_id] = worker
             worker.start()

    def handle_select_log_file(self, network_id): # Giống bản trước
         # ... (code similar to previous version) ...
         if not network_id or network_id not in self.networks_data: return
         current_path = self.networks_data[network_id].get('log_path', None)
         dir_path = os.path.dirname(current_path) if current_path else ""
         file_path, _ = QFileDialog.getSaveFileName(self, f"Select Log File for {self.networks_data[network_id]['name']}", dir_path, "*.csv")
         if file_path:
             if not file_path.lower().endswith(".csv"): file_path += ".csv"
             self.networks_data[network_id]['log_path'] = file_path
             self.networks_data[network_id]['log_message_count'] = 0 # Reset count when selecting new file
             self.networkDataUpdated.emit(network_id) # Update UI

    def handle_toggle_logging(self, network_id): # Cập nhật để dùng LoggingWorker
        if not network_id or network_id not in self.networks_data: return
        net_data = self.networks_data[network_id]
        is_currently_logging = net_data.get('is_logging', False)
        log_path = net_data.get('log_path', None)
        worker_id = f"{network_id}_logging"

        if not is_currently_logging: # Start Logging
             if not log_path:
                  QMessageBox.warning(self,"No Log File", "Please select a log file first.")
                  self.networkDataUpdated.emit(network_id) # Ensure button state resets
                  return

             # Dừng worker cũ nếu đang tồn tại và chạy (hiếm khi xảy ra)
             if worker_id in self.workers and self.workers[worker_id].isRunning():
                 self.workers[worker_id].stop()
                 self.workers[worker_id].wait(500)

             log_worker = LoggingWorker(network_id, log_path)
             log_worker.status.connect(self.update_network_status)
             log_worker.error.connect(self.show_network_error)
             log_worker.message_count.connect(self._update_log_count) # Update counter display
             net_data['is_logging'] = True
             net_data['logging_worker'] = log_worker
             net_data['log_message_count'] = 0 # Reset count on start
             self.workers[worker_id] = log_worker
             log_worker.start()
             self.statusLabel.setText(f"Net {net_data['name']}: Logging Started.")
        else: # Stop Logging
            log_worker = net_data.get('logging_worker')
            if log_worker and log_worker.isRunning():
                log_worker.stop() # Worker signals status when stopped
                self.statusLabel.setText(f"Net {net_data['name']}: Stopping logging...")
            net_data['is_logging'] = False
            net_data['logging_worker'] = None # Clear reference
            # Worker sẽ bị xóa khỏi self.workers khi thread kết thúc (nếu cần)

        self.networkDataUpdated.emit(network_id) # Update UI (button state)


    # --- Slots xử lý kết quả từ Worker (DBC, Trace) ---
    def on_dbc_loaded(self, network_id, db_or_none, path_or_error): # Cập nhật nhỏ
        worker_id = f"{network_id}_dbc"
        if worker_id in self.workers: del self.workers[worker_id]
        if network_id not in self.networks_data: return

        net_data = self.networks_data[network_id]
        if db_or_none:
            net_data['db'] = db_or_none
            net_data['dbc_path'] = path_or_error
            # Reset dependent data
            net_data['latest_signal_values'] = {}
            net_data['signal_time_series'] = {}
            # Cần re-decode live data hoặc re-populate file data nếu offline
            if net_data['connection_status'] == 'online':
                 self.statusLabel.setText(f"Net {net_data['name']}: DBC loaded. Live decoding active.")
                 # Re-decoding implicitly happens in handle_live_message
            else:
                 # If offline and trace data exists, repopulate trace table
                 self.statusLabel.setText(f"Net {net_data['name']}: DBC loaded.")
                 if net_data['trace_data']:
                      # Emit update to refresh Trace/Signal/Graph tabs with file data + new DBC
                      self.networkDataUpdated.emit(network_id)

        else: # Error loading DBC
            net_data['db'] = None
            net_data['dbc_path'] = None
            net_data['latest_signal_values'] = {}
            net_data['signal_time_series'] = {}
            self.show_network_error(network_id, f"Failed to load DBC:\n{path_or_error}")

        self.networkDataUpdated.emit(network_id) # Update UI regardless

    def on_trace_loaded(self, network_id, trace_data_or_none, signal_timeseries, path_or_error): # Giống bản trước
        worker_id = f"{network_id}_trace"
        if worker_id in self.workers: del self.workers[worker_id]
        if network_id not in self.networks_data: return

        net_data = self.networks_data[network_id]
        if trace_data_or_none is not None:
            net_data['trace_data'] = trace_data_or_none
            net_data['trace_path'] = path_or_error
            net_data['signal_time_series'] = signal_timeseries
            net_data['latest_signal_values'] = self._get_latest_values_from_timeseries(signal_timeseries)
            self.statusLabel.setText(f"Net {net_data['name']}: Trace file loaded.")
        else:
            net_data['trace_data'] = []
            net_data['trace_path'] = None
            net_data['signal_time_series'] = {}
            net_data['latest_signal_values'] = {}
            self.show_network_error(network_id, f"Failed to load Trace file:\n{path_or_error}")

        self.networkDataUpdated.emit(network_id) # Update relevant tabs

    def _get_latest_values_from_timeseries(self, signal_timeseries): # Giống bản trước
         latest_values = {}
         # ... (code similar to previous version) ...
         for sig_name, (timestamps, values) in signal_timeseries.items():
             if timestamps and values:
                  try:
                       # Assuming already sorted (worker should sort if needed)
                       latest_values[sig_name] = (values[-1], timestamps[-1])
                  except IndexError: pass # Handle empty lists edge case
         return latest_values

    def _update_log_count(self, network_id, count):
        """Slot to update log message count in the data and UI."""
        if network_id == self.current_selected_network_id:
            self.logTab.set_log_count(count)
        if network_id in self.networks_data:
            self.networks_data[network_id]['log_message_count'] = count


    # --- Trạng thái / Lỗi / Tiện ích ---
    def update_network_status(self, network_id, message): # Giống bản trước
        # ... (code similar to previous version) ...
        if network_id in self.networks_data:
            self.statusLabel.setText(f"Net {self.networks_data[network_id]['name']}: {message}")
        else: self.statusLabel.setText(message)

    def update_network_progress_percent(self, network_id, percent): # Giống bản trước
        # ... (code similar to previous version) ...
        if network_id in self.networks_data:
             self.statusLabel.setText(f"Net {self.networks_data[network_id]['name']}: Processing... {percent}%")

    def show_network_error(self, network_id, error_message): # Giống bản trước
        # ... (code similar to previous version) ...
        net_name = self.networks_data.get(network_id, {}).get('name', f"ID {network_id}")
        self.statusLabel.setText(f"Net {net_name}: Error occurred.")
        QMessageBox.critical(self, f"Error - Network {net_name}", error_message)

    def show_error_message(self, title, message): # Helper chung
        QMessageBox.critical(self, title, message)

    def show_about_dialog(self): # Giống bản trước
        # ... (code similar to previous version) ...
         QMessageBox.about(self, "About", "Multi-Network CAN Manager (Vector)\n\n...")


    # --- Xử lý Đóng Ứng dụng ---
    def closeEvent(self, event):
        """Ensures all connections are closed and threads are stopped."""
        print("Close event triggered...")
        disconnect_tasks = []
        # Identify active connections
        for net_id, net_data in self.networks_data.items():
            if net_data.get('connection_status') == 'online' or net_data.get('connection_status') == 'connecting':
                 disconnect_tasks.append(net_id)

        # Identify running non-listener workers (DBC, Trace, Log)
        other_workers = {wid: w for wid, w in self.workers.items()
                          if "_listener" not in wid and w.isRunning()}

        if disconnect_tasks or other_workers:
             tasks_running = []
             if disconnect_tasks: tasks_running.append(f"{len(disconnect_tasks)} active connection(s)")
             if other_workers: tasks_running.append(f"{len(other_workers)} background task(s)")

             reply = QMessageBox.question(self, 'Confirm Exit',
                                         f"The following are active:\n- {' and '.join(tasks_running)}"
                                         "\n\nExiting will attempt to disconnect and stop tasks. Continue?",
                                         QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
             if reply == QMessageBox.Yes:
                 print("Attempting to disconnect and stop workers...")
                 # Stop non-listener workers first
                 for wid, w in other_workers.items():
                     try:
                          if hasattr(w, 'stop'): w.stop()
                          else: w.quit()
                          if not w.wait(200): print(f"Warning: Worker {wid} did not stop quickly.")
                     except Exception as e: print(f"Error stopping worker {wid}: {e}")

                 # Request disconnect for active networks (this will stop listeners)
                 for net_id in disconnect_tasks:
                      self.disconnect_network(net_id)

                 # Add a small delay/wait loop to allow threads/disconnects to finalize
                 # This is tricky - a proper wait would be more complex.
                 # For simplicity, just give it a moment.
                 # Maybe check self.workers for remaining listener threads.
                 QApplication.processEvents() # Process pending events
                 time.sleep(0.5) # Give some time
                 # Re-check if listeners are still there
                 remaining_listeners = {wid for wid in self.workers if "_listener" in wid}
                 if remaining_listeners:
                     print(f"Warning: Listeners still pending stop: {remaining_listeners}")


                 event.accept()
             else:
                 event.ignore()
        else:
            event.accept() # No active connections or tasks

# --- Entry Point ---
if __name__ == '__main__':
    # Add this for better high-DPI scaling if needed
    # QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    # QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)

    app = QApplication(sys.argv)
    manager = MultiCanManagerApp()
    sys.exit(app.exec_())
