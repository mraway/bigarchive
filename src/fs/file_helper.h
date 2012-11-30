/*
 file_helper
*/

class FileHelper {
 public:
  FileHelper();
  void Open();
  void Read();
  void Write();
  void Close();
 protected:
  int fd;
  int mode; 
};
