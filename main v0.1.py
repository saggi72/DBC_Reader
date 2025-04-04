import sys
import os
import traceback # Để in thông tin lỗi chi tiết hơn

# Kiểm tra và cố gắng nhập cantools
try:
    import cantools
except ImportError:
    print("Lỗi: Thư viện 'cantools' chưa được cài đặt.")
    print("Vui lòng cài đặt bằng lệnh: pip install cantools")
    sys.exit(1)

# Kiểm tra và cố gắng nhập PyQt5
try:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout,
        QPushButton, QFileDialog, QTreeWidget, QTreeWidgetItem,
        QStatusBar, QMessageBox, QHeaderView
    )
    from PyQt5.QtCore import Qt, QThread, pyqtSignal
    from PyQt5.QtGui import QFont # Tùy chọn: để set font nếu muốn
except ImportError:
    print("Lỗi: Thư viện 'PyQt5' chưa được cài đặt.")
    print("Vui lòng cài đặt bằng lệnh: pip install PyQt5")
    sys.exit(1)


# --- (Tùy chọn) Lớp Worker để xử lý file DBC trong luồng nền ---
class DbcLoadingWorker(QThread):
    """
    Luồng xử lý nền để tải và phân tích file DBC, tránh làm đơ GUI.
    """
    # Tín hiệu phát ra: (database_object, file_path) hoặc (None, error_message)
    finished = pyqtSignal(object, str)

    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path

    def run(self):
        """Thực thi tác vụ tải file trong luồng."""
        try:
            # strict=False giúp bỏ qua một số lỗi không nghiêm trọng trong file DBC
            db = cantools.db.load_file(self.file_path, strict=False)
            self.finished.emit(db, self.file_path) # Thành công
        except FileNotFoundError:
            self.finished.emit(None, f"Lỗi: Không tìm thấy file '{self.file_path}'")
        except cantools.db.UnsupportedDatabaseFormatError as e:
             self.finished.emit(None, f"Lỗi định dạng DBC không được hỗ trợ:\n{e}\nFile: {self.file_path}")
        except Exception as e:
            # Bắt các lỗi chung khác từ cantools hoặc hệ thống file
            error_details = traceback.format_exc() # Lấy traceback chi tiết
            self.finished.emit(None, f"Lỗi khi đọc file DBC:\n{e}\n\nChi tiết:\n{error_details}\nFile: {self.file_path}")

# --- Lớp Cửa sổ Chính ---
class DBCViewerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = None # Lưu trữ database object đã parse
        self.current_file_path = "" # Lưu đường dẫn file hiện tại
        self.worker = None # Tham chiếu đến luồng xử lý nền
        self.initUI()

    def initUI(self):
        self.setWindowTitle("Trình Xem File DBC (cantools + PyQt5)")
        self.setGeometry(100, 100, 1000, 700) # Vị trí và kích thước cửa sổ ban đầu

        # --- Widget trung tâm và Layout ---
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # --- Nút Tải File ---
        self.load_button = QPushButton("Tải File DBC...")
        self.load_button.clicked.connect(self.open_file_dialog)
        layout.addWidget(self.load_button)

        # --- Widget Cây hiển thị dữ liệu DBC ---
        self.treeWidget = QTreeWidget()
        self.setup_tree_widget()
        layout.addWidget(self.treeWidget) # Thêm tree vào layout

        # --- Thanh Trạng Thái ---
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusBar.showMessage("Sẵn sàng. Chọn file DBC để bắt đầu.")

        self.show() # Hiển thị cửa sổ

    def setup_tree_widget(self):
        """Thiết lập các cột cho QTreeWidget."""
        headers = [
            "Tên / Mô tả",
            "ID (Hex)",
            "DLC",
            "Sender(s)", # Thêm cột sender
            "Cycle Time (ms)", # Thêm cột cycle time
            "Start Bit",
            "Length (bits)",
            "Byte Order",
            "Data Type",
            "Factor",
            "Offset",
            "Min",
            "Max",
            "Unit",
            "Receivers",
            "Comment" # Thêm cột comment
        ]
        self.treeWidget.setColumnCount(len(headers))
        self.treeWidget.setHeaderLabels(headers)

        # Cho phép chọn từng mục
        self.treeWidget.setSelectionMode(QTreeWidget.SingleSelection)

        # (Tùy chọn) Điều chỉnh cách cột thay đổi kích thước
        header = self.treeWidget.header()
        header.setSectionResizeMode(0, QHeaderView.Stretch) # Cột Tên sẽ tự co giãn
        # header.setSectionResizeMode(QHeaderView.ResizeToContents) # Cho tất cả các cột khác

        # (Tùy chọn) Thiết lập font chữ nếu muốn
        # font = QFont("Segoe UI", 9)
        # self.treeWidget.setFont(font)
        # header.setFont(font)

    def open_file_dialog(self):
        """Mở hộp thoại chọn file và bắt đầu quá trình tải."""
        options = QFileDialog.Options()
        # options |= QFileDialog.DontUseNativeDialog # Nếu muốn dùng dialog chuẩn Qt
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn File DBC",
            "", # Thư mục bắt đầu (để trống là thư mục làm việc hiện tại)
            "CAN Database Files (*.dbc);;All Files (*)", # Bộ lọc file
            options=options
        )

        if file_path:
            self.statusBar.showMessage(f"Đang tải file: {os.path.basename(file_path)}...")
            self.treeWidget.clear() # Xóa cây cũ trước khi tải mới
            self.load_button.setEnabled(False) # Vô hiệu hóa nút trong khi tải

            # --- Sử dụng luồng nền ---
            self.worker = DbcLoadingWorker(file_path)
            self.worker.finished.connect(self.on_loading_finished)
            self.worker.start()

            # --- (Cách xử lý trực tiếp - Không khuyến khích cho file lớn) ---
            # self.load_dbc(file_path)
            # self.load_button.setEnabled(True) # Bật lại nút nếu xử lý trực tiếp

    def on_loading_finished(self, db_or_none, path_or_error_msg):
        """Slot xử lý kết quả từ luồng tải file DBC."""
        self.load_button.setEnabled(True) # Bật lại nút tải
        self.worker = None # Xóa tham chiếu đến worker cũ

        if db_or_none is not None:
            self.db = db_or_none
            self.current_file_path = path_or_error_msg
            file_name = os.path.basename(self.current_file_path)
            self.setWindowTitle(f"Trình Xem File DBC - {file_name}")
            self.statusBar.showMessage(f"Đã tải thành công: {self.current_file_path} | Phiên bản DBC: {self.db.version if self.db.version else 'N/A'}")
            self.populate_tree_widget()
        else:
            # Có lỗi xảy ra
            error_message = path_or_error_msg
            self.show_error_message("Lỗi Tải File DBC", error_message)
            self.statusBar.showMessage("Tải file thất bại.")
            self.setWindowTitle("Trình Xem File DBC") # Reset tiêu đề

    # --- (Hàm load_dbc nếu xử lý trực tiếp) ---
    # def load_dbc(self, file_path):
    #     """Tải và phân tích file DBC (xử lý trực tiếp)."""
    #     try:
    #         self.db = cantools.db.load_file(file_path, strict=False)
    #         self.current_file_path = file_path
    #         file_name = os.path.basename(file_path)
    #         self.setWindowTitle(f"Trình Xem File DBC - {file_name}")
    #         self.statusBar.showMessage(f"Đã tải thành công: {self.current_file_path} | Version: {self.db.version}")
    #         self.populate_tree_widget()
    #     except FileNotFoundError:
    #         self.show_error_message("Lỗi", f"Không tìm thấy file: {file_path}")
    #         self.statusBar.showMessage("Lỗi: Không tìm thấy file.")
    #     except Exception as e:
    #         self.show_error_message("Lỗi Phân Tích DBC", f"Không thể phân tích file DBC:\n{e}\n\nFile: {file_path}")
    #         self.statusBar.showMessage("Lỗi phân tích file DBC.")
    #     finally:
    #           self.load_button.setEnabled(True) # Đảm bảo nút được bật lại

    def populate_tree_widget(self):
        """Điền dữ liệu từ file DBC đã phân tích vào QTreeWidget."""
        if not self.db:
            return

        self.treeWidget.clear() # Xóa nội dung cũ

        # Tạo danh sách các node gửi duy nhất để làm mục cấp cao nhất
        # Sắp xếp theo tên node
        sender_nodes = sorted(list(set(sender for msg in self.db.messages for sender in msg.senders)))
        # Hoặc lấy từ danh sách node nếu có và cần thiết
        # defined_nodes = sorted([node.name for node in self.db.nodes])

        # Nhóm message theo người gửi để dễ tạo cây
        messages_by_sender = {node_name: [] for node_name in sender_nodes}
        all_messages = sorted(self.db.messages, key=lambda m: m.frame_id) # Sắp xếp message theo ID

        for msg in all_messages:
             # Một message có thể không có sender trong một số file DBC không chuẩn
             if not msg.senders:
                 # Có thể thêm vào một nhóm "Không xác định" hoặc bỏ qua
                 # Ở đây, ta sẽ tạo mục message trực tiếp dưới gốc nếu không có sender
                 if "__NO_SENDER__" not in messages_by_sender:
                    messages_by_sender["__NO_SENDER__"] = []
                 messages_by_sender["__NO_SENDER__"].append(msg)
                 if "__NO_SENDER__" not in sender_nodes:
                     sender_nodes.append("__NO_SENDER__") # Thêm node ảo vào danh sách
             else:
                 for sender in msg.senders:
                     if sender in messages_by_sender: # Đảm bảo sender có trong danh sách node (từ file DBC)
                         messages_by_sender[sender].append(msg)


        # Duyệt qua các Node (người gửi)
        for node_name in sender_nodes:
             display_node_name = node_name if node_name != "__NO_SENDER__" else "[Không rõ Sender]"
             node_item = QTreeWidgetItem(self.treeWidget, [display_node_name])
             node_item.setData(0, Qt.ItemDataRole.UserRole, {"type": "node"}) # Lưu loại item

             # Lấy comment của node nếu có
             node_obj = self.db.get_node_by_name(node_name)
             if node_obj and node_obj.comment:
                  node_item.setText(15, str(node_obj.comment)) # Cột Comment


             # Duyệt qua các Message mà Node này gửi
             # Sắp xếp message theo ID trong nhóm của node
             sorted_messages_for_node = sorted(messages_by_sender.get(node_name, []), key=lambda m: m.frame_id)

             for msg in sorted_messages_for_node:
                # --- Tạo Item cho Message ---
                frame_id_hex = f"0x{msg.frame_id:X}"
                senders_str = ", ".join(msg.senders) if msg.senders else "N/A"
                cycle_time_str = str(msg.cycle_time) if msg.cycle_time is not None else ""
                comment_str = str(msg.comment) if msg.comment else ""

                message_item_data = [
                    f"{msg.name}",                 # 0: Tên
                    frame_id_hex,                  # 1: ID (Hex)
                    str(msg.length),               # 2: DLC
                    senders_str,                   # 3: Sender(s)
                    cycle_time_str,                # 4: Cycle Time
                    "",                            # 5: Start Bit (Trống cho Message)
                    "",                            # 6: Length (Trống cho Message)
                    "",                            # 7: Byte Order (Trống cho Message)
                    "",                            # 8: Data Type (Trống cho Message)
                    "",                            # 9: Factor (Trống cho Message)
                    "",                            # 10: Offset (Trống cho Message)
                    "",                            # 11: Min (Trống cho Message)
                    "",                            # 12: Max (Trống cho Message)
                    "",                            # 13: Unit (Trống cho Message)
                    "",                             # 14: Receivers (Trống cho message tổng thể)
                    comment_str                    # 15: Comment
                ]
                message_item = QTreeWidgetItem(node_item, message_item_data)
                message_item.setData(0, Qt.ItemDataRole.UserRole, {"type": "message", "id": msg.frame_id})


                # Sắp xếp Signal theo Start Bit
                sorted_signals = sorted(msg.signals, key=lambda s: s.start)

                # Duyệt qua các Signal trong Message
                for sig in sorted_signals:
                    byte_order_str = "Little Endian" if sig.byte_order == 'little_endian' else "Big Endian"
                    type_str = "Signed" if sig.is_signed else "Unsigned"
                    factor_str = f"{sig.scale:.10g}" # Định dạng số thực, bỏ số 0 thừa
                    offset_str = f"{sig.offset:.10g}"
                    min_str = f"{sig.minimum:.10g}" if sig.minimum is not None else ""
                    max_str = f"{sig.maximum:.10g}" if sig.maximum is not None else ""
                    unit_str = str(sig.unit) if sig.unit else ""
                    receivers_str = ", ".join(sig.receivers) if sig.receivers else ""
                    comment_sig_str = str(sig.comment) if sig.comment else ""


                    # --- Tạo Item cho Signal ---
                    signal_item_data = [
                        f"  └─ {sig.name}",      # 0: Tên (Thụt vào)
                        "",                         # 1: ID (Trống cho Signal)
                        "",                         # 2: DLC (Trống cho Signal)
                        "",                         # 3: Senders (Trống cho Signal)
                        "",                         # 4: Cycle Time (Trống cho Signal)
                        str(sig.start),             # 5: Start Bit
                        str(sig.length),            # 6: Length (bits)
                        byte_order_str,             # 7: Byte Order
                        type_str,                   # 8: Data Type
                        factor_str,                 # 9: Factor
                        offset_str,                 # 10: Offset
                        min_str,                    # 11: Min
                        max_str,                    # 12: Max
                        unit_str,                   # 13: Unit
                        receivers_str,              # 14: Receivers
                        comment_sig_str             # 15: Comment
                    ]
                    signal_item = QTreeWidgetItem(message_item, signal_item_data)
                    signal_item.setData(0, Qt.ItemDataRole.UserRole, {"type": "signal"}) # Lưu loại item

        # Tự động điều chỉnh độ rộng các cột sau khi điền dữ liệu
        # Ngoại trừ cột đầu tiên đã được đặt là Stretch
        for i in range(1, self.treeWidget.columnCount()):
            self.treeWidget.resizeColumnToContents(i)

        # Mở rộng tất cả các node cấp cao nhất (tùy chọn)
        # self.treeWidget.expandAll()
        # Chỉ mở rộng các Node
        for i in range(self.treeWidget.topLevelItemCount()):
             self.treeWidget.topLevelItem(i).setExpanded(True)


    def show_error_message(self, title, message):
        """Hiển thị hộp thoại thông báo lỗi."""
        QMessageBox.critical(self, title, message)

    def closeEvent(self, event):
        """Xử lý sự kiện đóng cửa sổ."""
        # Đảm bảo luồng nền dừng lại nếu đang chạy
        if self.worker and self.worker.isRunning():
             # Không thể dừng trực tiếp một cách an toàn, chỉ cảnh báo
             # hoặc chờ một cách cẩn thận (phức tạp hơn)
             print("Đang cố gắng đóng khi luồng đang chạy...")
             # Có thể hỏi người dùng xác nhận
             reply = QMessageBox.question(self, 'Thoát Ứng Dụng',
                                         'Đang có tác vụ xử lý file DBC. Bạn có chắc muốn thoát?',
                                         QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
             if reply == QMessageBox.Yes:
                 # Có thể cần cơ chế phức tạp hơn để hủy luồng một cách an toàn
                 event.accept()
             else:
                 event.ignore()
        else:
             event.accept() # Chấp nhận sự kiện đóng


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = DBCViewerApp()
    sys.exit(app.exec_())
