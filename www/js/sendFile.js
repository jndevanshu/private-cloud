$(document).ready(function(){
  $("#send-file-form").on('submit', function(e){
    e.preventDefault();
    var file = document.getElementById("fileToUpload").files[0];
    var reader = new FileReader();

    reader.readAsDataURL(file);

    reader.addEventListener("load", function(){
      var request = new XMLHttpRequest();
      request.open('POST', '/cgi-bin/sendFile', /* async = */ false);
      var formData = new FormData(document.getElementById('send-file-form'));
      console.log(file);
      console.log(reader.result.split(",")[1]);
      var file2 = new File([reader.result.split(",")[1]], file.name, {"type":file.type});
      formData.set("fileToUpload", file2, file2.name);
      console.log(formData);
      $.ajax({
        type: "POST",
        url: "/cgi-bin/sendFile",
        data: formData,
        contentType: false,
        processData: false,
        error: function(){
          alert("some error occured");
        },
        success: function(data){
          console.log(data);
          alert(data);
          window.location.href = "/";
        }
      });
    });
  });
});