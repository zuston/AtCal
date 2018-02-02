
document.body.onclick = function(event){    //冒泡处理
    //var btn1=document.getElementById('btn');
    var detail1in=document.getElementById('detail1in');
    var detail2in=document.getElementById('detail2in');
    var detail3in=document.getElementById('detail3in');
    var detail4in=document.getElementById('detail4in');
    var detail5in=document.getElementById('detail5in');
    var detail1out=document.getElementById('detail1out');
    var detail2out=document.getElementById('detail2out');
    var detail3out=document.getElementById('detail3out');
    var detail4out=document.getElementById('detail4out');
    var detail5out=document.getElementById('detail5out');



    var id = event.target.id;
    console.log(id);
    if(id ==="mydetail1in"){
        detail1in.style.display='block';
        detail2in.style.display='none';
        detail3in.style.display='none';
        detail4in.style.display='none';
        detail5in.style.display='none';

        detail1out.style.display='none';
        detail2out.style.display='none';
        detail3out.style.display='none';
        detail4out.style.display='none';
        detail5out.style.display='none';
    }
    else if(id === "mydetail2in"){
        detail1in.style.display='none';
        detail2in.style.display='block';
        detail3in.style.display='none';
        detail4in.style.display='none';
        detail5in.style.display='none';

        detail1out.style.display='none';
        detail2out.style.display='none';
        detail3out.style.display='none';
        detail4out.style.display='none';
        detail5out.style.display='none';

    }
    else if(id ==="mydetail3in"){
        detail1in.style.display='none';
        detail2in.style.display='none';
        detail3in.style.display='block';
        detail4in.style.display='none';
        detail5in.style.display='none';

        detail1out.style.display='none';
        detail2out.style.display='none';
        detail3out.style.display='none';
        detail4out.style.display='none';
        detail5out.style.display='none';

    }
    else if (id==="mydetail4in"){
        detail1in.style.display='none';
        detail2in.style.display='none';
        detail3in.style.display='none';
        detail4in.style.display='block';
        detail5in.style.display='none';

        detail1out.style.display='none';
        detail2out.style.display='none';
        detail3out.style.display='none';
        detail4out.style.display='none';
        detail5out.style.display='none';

    }
    else if (id==="mydetail5in"){
        detail1in.style.display='none';
        detail2in.style.display='none';
        detail3in.style.display='none';
        detail4in.style.display='none';
        detail5in.style.display='block';

        detail1out.style.display='none';
        detail2out.style.display='none';
        detail3out.style.display='none';
        detail4out.style.display='none';
        detail5out.style.display='none';

    }
    else if(id ==="mydetail1out"){
        detail1in.style.display='none';
        detail2in.style.display='none';
        detail3in.style.display='none';
        detail4in.style.display='none';
        detail5in.style.display='none';

        detail1out.style.display='block';
        detail2out.style.display='none';
        detail3out.style.display='none';
        detail4out.style.display='none';
        detail5out.style.display='none';
    }
    else if(id === "mydetail2out"){
        detail1in.style.display='none';
        detail2in.style.display='none';
        detail3in.style.display='none';
        detail4in.style.display='none';
        detail5in.style.display='none';

        detail1out.style.display='none';
        detail2out.style.display='block';
        detail3out.style.display='none';
        detail4out.style.display='none';
        detail5out.style.display='none';

    }
    else if(id ==="mydetail3out"){
        detail1in.style.display='none';
        detail2in.style.display='none';
        detail3in.style.display='none';
        detail4in.style.display='none';
        detail5in.style.display='none';

        detail1out.style.display='none';
        detail2out.style.display='none';
        detail3out.style.display='block';
        detail4out.style.display='none';
        detail5out.style.display='none';

    }
    else if (id==="mydetail4out"){
        detail1in.style.display='none';
        detail2in.style.display='none';
        detail3in.style.display='none';
        detail4in.style.display='none';
        detail5in.style.display='none';

        detail1out.style.display='none';
        detail2out.style.display='none';
        detail3out.style.display='none';
        detail4out.style.display='block';
        detail5out.style.display='none';

    }
    else if (id==="mydetail5out"){
        detail1in.style.display='none';
        detail2in.style.display='none';
        detail3in.style.display='none';
        detail4in.style.display='none';
        detail5in.style.display='none';

        detail1out.style.display='none';
        detail2out.style.display='none';
        detail3out.style.display='none';
        detail4out.style.display='none';
        detail5out.style.display='block';

    }




};




