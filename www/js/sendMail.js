$(document).ready(function(){
  $("#send-mail-form").on('submit', function(e){
    e.preventDefault();
    var file = document.getElementById("attachment").files[0];
    var reader = new FileReader();

    if(file){
      reader.readAsDataURL(file);
    }
    else{
      var formData = new FormData(document.getElementById('send-mail-form'));
      sendMail(formData);
    }
    reader.addEventListener("load", function(){
      var request = new XMLHttpRequest();
      request.open('POST', '/cgi-bin/sendMail', /* async = */ false);
      var formData = new FormData(document.getElementById('send-mail-form'));
      console.log(file);
      console.log(reader.result.split(",")[1]);
      var file2 = new File([reader.result.split(",")[1]], file.name, {"type":file.type});
      formData.set("attachment", file2, file2.name);
      sendMail(formData);
    });
  });

  var sendMail = function(formData){
    formData.set("message", $("#message").val());
    $.ajax({
        type: "POST",
        url: "/cgi-bin/sendMail",
        data: formData,
        contentType: false,
        processData: false,
        error: function(data){
          console.log(data);
          alert("some error occured: " + data);
        },
        success: function(data){
          console.log(data);
          alert(data);
          window.location.href = "/";
        }
      });
  }
});