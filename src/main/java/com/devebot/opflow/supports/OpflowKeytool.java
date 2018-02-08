package com.devebot.opflow.supports;

import com.devebot.opflow.OpflowLogTracer;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowKeytool {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowKeytool.class);
    private final static OpflowLogTracer logTracer = OpflowLogTracer.ROOT.copy();
    
    public static SSLContext buildSSLContextWithCertFile(String pkcs12File, String pkcs12Passphrase, String caCertFile) {
        try {
            KeyStore tks = KeyStore.getInstance("JKS");
            tks.load(null);
            tks.setCertificateEntry("cert", loadCertificate(caCertFile));
            return buildSSLContext(pkcs12File, pkcs12Passphrase, tks);
        }
        catch (IOException e) {
            logException(e);
        }
        catch (KeyStoreException e) {
            logException(e);
        }
        catch (NoSuchAlgorithmException e) {
            logException(e);
        }
        catch (CertificateException e) {
            logException(e);
        }
        return null;
    }
    
    public static SSLContext buildSSLContextWithKeyStore(String pkcs12File, String pkcs12Passphrase, String trustStoreFile, String trustPassphrase) {
        try {
            char[] passphrase = trustPassphrase.toCharArray();
            KeyStore tks = KeyStore.getInstance("JKS");
            tks.load(new FileInputStream(trustStoreFile), passphrase);
            return buildSSLContext(pkcs12File, pkcs12Passphrase, tks);
        }
        catch (IOException e) {
            logException(e);
        }
        catch (KeyStoreException e) {
            logException(e);
        }
        catch (NoSuchAlgorithmException e) {
            logException(e);
        }
        catch (CertificateException e) {
            logException(e);
        }
        return null;
    }
    
    public static SSLContext buildSSLContext(String pkcs12File, String pkcs12Passphrase, KeyStore tks) {
        try {
            char[] keyPassphrase = pkcs12Passphrase.toCharArray();
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(new FileInputStream(pkcs12File), keyPassphrase);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, keyPassphrase);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(tks);

            SSLContext c = SSLContext.getInstance("TLSv1.2");
            c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            
            return c;
        }
        catch (IOException e) {
            logException(e);
        }
        catch (KeyStoreException e) {
            logException(e);
        }
        catch (NoSuchAlgorithmException e) {
            logException(e);
        }
        catch (CertificateException e) {
            logException(e);
        }
        catch (UnrecoverableKeyException e) {
            logException(e);
        }
        catch (KeyManagementException e) {
            logException(e);
        }
        return null;
    }
    
    private static Certificate loadCertificate(String caCertFile) throws CertificateException, FileNotFoundException {
        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        FileInputStream is = new FileInputStream(caCertFile);
        Certificate cer = (Certificate) fact.generateCertificate(is);
        return cer;
    }
    
    private static void logException(Exception e) {
        if (OpflowLogTracer.has(LOG, "error")) LOG.error(logTracer
                .put("exceptionClass", e.getClass().getName())
                .put("exceptionMessage", e.getMessage())
                .text("Instance[${instanceId}] buildSSLContext() exception[${exceptionClass}]: ${exceptionMessage}")
                .stringify());
    }
}
