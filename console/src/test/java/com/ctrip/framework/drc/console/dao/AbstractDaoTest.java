package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;

/**
 * Created by shiruixin
 * 2024/8/28 16:52
 */
public class AbstractDaoTest {
    @Mock
    DefaultConsoleConfig consoleConfig;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testAllOverseaDao() throws Exception {
        MockedStatic<DefaultConsoleConfig> mockedStatic = Mockito.mockStatic(DefaultConsoleConfig.class);
        mockedStatic.when(DefaultConsoleConfig::getInstance).thenReturn(consoleConfig);

        Mockito.when(consoleConfig.getRegion()).thenReturn("sin");
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Collections.singleton("sin"));

        String packagePath = "com.ctrip.framework.drc.console.dao";
        int daoReturnExceptionCnt = 0;

        List<Class<?>> daoClasses = new DaoClassScanner().scanDaoClasses(packagePath);
        for(Class<?> clazz: daoClasses) {
            Object daoObject = clazz.newInstance();
            try {
                Method countMethod = clazz.getMethod("queryByIds",List.class);
                countMethod.invoke(daoObject, Arrays.asList(1L));
            } catch (Exception e) {
                if (e.getCause() instanceof UnsupportedOperationException) {
                    daoReturnExceptionCnt++;
                }
            }
        }
        Assert.assertEquals(daoReturnExceptionCnt, daoClasses.size());
    }

    class DaoClassScanner {
        public List<Class<?>> scanDaoClasses(String packageName) throws IOException, ClassNotFoundException {
            List<Class<?>> daoClasses = new ArrayList<>();

            String packagePath = packageName.replace('.', '/');
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Enumeration<URL> resources = classLoader.getResources(packagePath);

            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                File file = new File(resource.getFile());
                scanDaoClassesInDirectory(packageName, file, daoClasses);
            }

            return daoClasses;
        }

        private void scanDaoClassesInDirectory(String packageName, File directory, List<Class<?>> daoClasses) throws ClassNotFoundException {
            if (directory.exists()) {
                File[] files = directory.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isDirectory()) {
                            scanDaoClassesInDirectory(packageName + "." + file.getName(), file, daoClasses);
                        } else if (file.getName().endsWith(".class")) {
                            String className = packageName + '.' + file.getName().substring(0, file.getName().length() - 6);
                            Class<?> clazz = Class.forName(className);
                            if (isDaoClass(clazz)) {
                                daoClasses.add(clazz);
                            }
                        }
                    }
                }
            }
        }

        private boolean isDaoClass(Class<?> clazz) {
            return clazz.getSimpleName().endsWith("Dao")
                    &&  !"AbstractDao".equals(clazz.getSimpleName())
                    && !"OverseaDalTableDao".equals(clazz.getSimpleName())
                    && !"BaseDalTableDao".equals(clazz.getSimpleName());
        }
    }
}
