package io.github.zuston.webService.Controller;

import io.github.zuston.webService.Service.ApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by zuston on 2018/1/18.
 */

@RestController
@RequestMapping("/atcal")
public class ApiController {

    @Autowired
    private ApiService apiService;

    @RequestMapping(value = "/currentinfo",method = RequestMethod.GET)
    public String siteTraceInfo(){
        return apiService.siteTraceInfo();
    }

    @RequestMapping(value = "/siteinfo", method = RequestMethod.GET)
    public String siteInfo(@RequestParam("siteId")long siteId, @RequestParam("size")int size, @RequestParam("tag")int tag){
        return apiService.siteInfo(siteId, size, tag);
    }

    @RequestMapping(value = "/traceinfo", method = RequestMethod.GET)
    public String traceInfo(){
        return null;
    }

}
