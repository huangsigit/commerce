package com.egao.common.system.controller;

import com.egao.common.core.UploadConstant;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.web.JsonResult;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.CertificateService;
import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

/*
@Api(value = "证书管理", tags = "certificate")
@RestController
@RequestMapping("/certificate")
*/

@Controller
@RequestMapping("/sys/certificate")
public class CertificateController {

//    private static final int UPLOAD_DIS_INDEX = 0;  // 上传到第几个磁盘下面
//    private static final String UPLOAD_DIR = "/upload/";  // 上传的目录
//    private static final boolean UUID_NAME = false;  // 是否用uuid命名

//    public static final String FILE_DIR = "certificate/";  // 上传的目录

    @Autowired
    private CertificateService certificateService;

    @Autowired
    private AdService adService;


    @RequiresPermissions("sys:certificate:view")
    @RequestMapping()
    public String view() {
        return "system/certificate.html";
    }


/*
    @ApiOperation(value = "分页查询证书")
    @PreAuthorize("hasAuthority('get:/certificate')")
    @GetMapping()
*/


    @OperLog(value = "证书管理", desc = "分页查询")
    @RequiresPermissions("sys:certificate:list")
    @ResponseBody
    @RequestMapping("/page")
    public JsonResult list(HttpServletRequest request, @RequestParam(name = "keyword", required = false)String keyword) {

        System.out.println("keyword："+keyword);

        Map map = new HashMap<>();
        map.put("keyword", keyword);
        List<Map<String, Object>> list = certificateService.selectCertificate(map);
        JsonResult data = JsonResult.ok(0, list.size(),"成功").put("data", list);

        return data;
    }


/*
    @ApiOperation(value = "添加证书")
    @PreAuthorize("hasAuthority('post:/certificate')")
    @PostMapping()
*/

    /**
     * 添加数据
     */
    @OperLog(value = "证书管理", desc = "添加证书", result = true)
    @RequiresPermissions("sys:certificate:add")
    @ResponseBody
    @RequestMapping("/add")
    public JsonResult add(HttpServletRequest request, @RequestParam(required=false) MultipartFile file,
                          @RequestParam(name = "id", required = false)Long id,
                          @RequestParam(name = "username", required = false)String username,
                          @RequestParam(name = "serviceAccountId", required = false)String serviceAccountId) {
        try {
            System.out.println("add certificate:"+file);

            String originalFileName = file.getOriginalFilename();
            System.out.println("originalFileName："+originalFileName);

            System.out.println("serviceAccountId："+serviceAccountId);

            Date date = new Date(System.currentTimeMillis());
            System.out.println("date:"+date);

            String path;  // 文件路径
            // 文件原始名称
            String suffix = originalFileName.substring(originalFileName.lastIndexOf(".") + 1);  // 获取文件后缀
            File outFile;
            if (UploadConstant.UUID_NAME) {  // uuid命名
                path = getDate() + UUID.randomUUID().toString().replaceAll("-", "") + "." + suffix;
                outFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);
            } else {  // 使用原名称，存在相同着加(1)
                String prefix = originalFileName.substring(0, originalFileName.lastIndexOf("."));  // 获取文件名称
                path = getDate() + originalFileName;
                outFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);
                int sameSize = 1;
                while (outFile.exists()) {
                    path = getDate() + prefix + "(" + sameSize + ")." + suffix;
                    outFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);
                    sameSize++;
                }
            }


            Map map = new HashMap();
            map.put("id", id);
            map.put("username", username);
            map.put("service_account_id", serviceAccountId);
            map.put("name", originalFileName);
            map.put("path", path);

            map.put("create_time", DateUtil.getCurrentDate());
//            map.put("create_time", new Date());

            System.out.println("map:"+map);
            certificateService.insertCertificate(map);


            System.out.println("path："+path);
            try {
                if (!outFile.getParentFile().exists()) {
                    outFile.getParentFile().mkdirs();
                }
                file.transferTo(outFile);
            } catch (Exception e) {
                e.printStackTrace();
                return JsonResult.error("上传失败").put("error", e.getMessage());
            }







            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("添加失败");
        }
    }

    /**
     * 获取当前日期
     */
    private String getDate() {
        return getDate("yyyy/MM/dd/");
    }

    private String getDate(String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date());
    }


/*
    @ApiOperation(value = "修改证书")
    @PreAuthorize("hasAuthority('put:/certificate')")
    @PutMapping()
*/


    /**
     * 修改数据
     */
    @OperLog(value = "证书管理", desc = "修改", result = true)
    @RequiresPermissions("sys:certificate:add")
    @ResponseBody
    @RequestMapping("/update")
    public JsonResult update(HttpServletRequest request, @RequestParam(required=false) MultipartFile file,
                             @RequestParam(name = "id", required = false)Long id,
                             @RequestParam(name = "username", required = false)String username,
                             @RequestParam(name = "serviceAccountId", required = false)Long serviceAccountId) {
        try {

            System.out.println("update certificate:"+file);
            System.out.println("update id:"+id);

            String originalFileName = file.getOriginalFilename();
            System.out.println("originalFileName："+originalFileName);

            System.out.println("serviceAccountId："+serviceAccountId);

            Date date = new Date(System.currentTimeMillis());
            System.out.println("date:"+date);

            String path;  // 文件路径
            // 文件原始名称
            String suffix = originalFileName.substring(originalFileName.lastIndexOf(".") + 1);  // 获取文件后缀
            File outFile;
            if (UploadConstant.UUID_NAME) {  // uuid命名
                path = getDate() + UUID.randomUUID().toString().replaceAll("-", "") + "." + suffix;
                outFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);
            } else {  // 使用原名称，存在相同着加(1)
                String prefix = originalFileName.substring(0, originalFileName.lastIndexOf("."));  // 获取文件名称
                path = getDate() + originalFileName;
                outFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);
                int sameSize = 1;
                while (outFile.exists()) {
                    path = getDate() + prefix + "(" + sameSize + ")." + suffix;
                    outFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);
                    sameSize++;
                }
            }


            Map map = new HashMap();
            map.put("id", id);
            map.put("username", username);
            map.put("service_account_id", serviceAccountId);
            map.put("name", originalFileName);
            map.put("path", path);
            map.put("create_time", DateUtil.getCurrentDate());
//            map.put("create_time", new Date());

            System.out.println("map:"+map);
            certificateService.updateCertificate(map);


            System.out.println("path："+path);
            try {
                if (!outFile.getParentFile().exists()) {
                    outFile.getParentFile().mkdirs();
                }
                file.transferTo(outFile);
            } catch (Exception e) {
                e.printStackTrace();
                return JsonResult.error("上传失败").put("error", e.getMessage());
            }

            return JsonResult.ok("修改成功");
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("修改失败");
        }
    }


/*
    @ApiOperation(value = "删除证书")
    @PreAuthorize("hasAuthority('delete:/certificate/{id}')")
    @DeleteMapping("/{id}")
*/


    /**
     * 修改数据
     */
    @OperLog(value = "证书管理", desc = "删除", result = true)
    @RequiresPermissions("sys:certificate:delete")
    @ResponseBody
    @RequestMapping("/delete")
    public JsonResult delete(@PathVariable("id") Long certificateId) {

        System.out.println("delete certificateId："+certificateId);
        try {



//            File orgFile = new File(File.listRoots()[UPLOAD_DIS_INDEX], UPLOAD_DIR + "2020/04/16/HelloAnalytics(1).java");
//            System.out.println("orgFile:" + orgFile.getPath());


            Map<String, Object> certificateMap = certificateService.selectCertificateById(certificateId);
            if(certificateMap != null){
                String path = (String)certificateMap.get("path");
                if(StringUtils.isNotEmpty(path)){
                    File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);
                    System.out.println("orgFile:" + orgFile.getPath());
                    orgFile.delete();
//                    return JsonResult.ok("删除成功");
                }
            }

            certificateService.deleteCertificate(certificateId);
            return JsonResult.ok("删除成功");
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("删除失败");
        }
    }


/*
    @ApiOperation(value = "查询所有证书")
    @PreAuthorize("hasAuthority('get:/certificate/all')")
    @GetMapping("/all")
*/

    /**
     * 查询所有证书
     */
    @OperLog(value = "证书管理", desc = "查询所有证书", result = true)
    @RequiresPermissions("sys:certificate:list")
    @ResponseBody
    @RequestMapping("/all")
    public JsonResult listAll() {

        /*
        List<Menu> list = menuService.list();
        System.out.println("certificate list："+list);
        JsonResult data = JsonResult.ok().put("data", list);
        System.out.println("data:"+ JSONObject.toJSON(data));

        return data;
        */
        return null;
    }

}
